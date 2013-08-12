/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexHFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {

  static Log LOG = LogFactory.getLog(IndexHFileOutputFormat.class);
  static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";
  private static final String DATABLOCK_ENCODING_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.datablock.encoding";
  TimeRangeTracker trt = new TimeRangeTracker();

  public static void configureIncrementalLoad(Job job, HTable table) throws IOException {
    HFileOutputFormat.configureIncrementalLoad(job, table);
    // Override OutputFormatClass
    job.setOutputFormatClass(IndexHFileOutputFormat.class);
  }

  public RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
      final TaskAttemptContext context) throws IOException, InterruptedException {

    // Get the path of the temporary output file
    final Path outputPath = FileOutputFormat.getOutputPath(context);
    final Path outputdir = new FileOutputCommitter(outputPath, context).getWorkPath();

    final Configuration conf = context.getConfiguration();
    final FileSystem fs = outputdir.getFileSystem(conf);

    // These configs. are from hbase-*.xml
    final long maxsize =
        conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE);
    final int blocksize =
        conf.getInt("hbase.mapreduce.hfileoutputformat.blocksize", HFile.DEFAULT_BLOCKSIZE);
    // Invented config. Add to hbase-*.xml if other than default compression.
    final String defaultCompression =
        conf.get("hfile.compression", Compression.Algorithm.NONE.getName());
    final boolean compactionExclude =
        conf.getBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude", false);

    final boolean indexedTable = conf.getBoolean(IndexMapReduceUtil.INDEX_IS_INDEXED_TABLE, false);

    final Path indexDir = new Path(outputdir, IndexMapReduceUtil.INDEX_DATA_DIR);

    final FileSystem indexFs = indexDir.getFileSystem(conf);

    if (indexedTable) {
      if (!indexFs.exists(indexDir)) {
        indexFs.mkdirs(indexDir);
      }
    }

    // create a map from column family to the compression algorithm
    final Map<byte[], String> compressionMap = createFamilyCompressionMap(conf);

    String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_CONF_KEY);
    final HFileDataBlockEncoder encoder;
    if (dataBlockEncodingStr == null) {
      encoder = NoOpDataBlockEncoder.INSTANCE;
    } else {
      try {
        encoder = new HFileDataBlockEncoderImpl(DataBlockEncoding.valueOf(dataBlockEncodingStr));
      } catch (IllegalArgumentException ex) {
        throw new RuntimeException("Invalid data block encoding type configured for the param "
            + DATABLOCK_ENCODING_CONF_KEY + " : " + dataBlockEncodingStr);
      }
    }

    return new RecordWriter<ImmutableBytesWritable, KeyValue>() {
      // Map of families to writers and how much has been output on the writer.
      private final Map<byte[], WriterLength> writers = new TreeMap<byte[], WriterLength>(
          Bytes.BYTES_COMPARATOR);
      private byte[] previousRow = HConstants.EMPTY_BYTE_ARRAY;
      private final byte[] now = Bytes.toBytes(System.currentTimeMillis());
      private boolean rollRequested = false;

      public void write(ImmutableBytesWritable row, KeyValue kv) throws IOException {
        // null input == user explicitly wants to flush
        if (row == null && kv == null) {
          rollWriters();
          return;
        }
        boolean indexed = false;

        byte[] rowKey = kv.getRow();
        long length = kv.getLength();
        byte[] family = kv.getFamily();
        byte[] qualifier = kv.getQualifier();

        if (Bytes.equals(family, Constants.IDX_COL_FAMILY)
            && Bytes.equals(qualifier, Constants.IDX_COL_QUAL)) {
          indexed = true;
        }

        WriterLength wl = null;
        if (indexed) {
          wl = this.writers.get(Bytes.toBytes(IndexMapReduceUtil.INDEX_DATA_DIR));
        } else {
          wl = this.writers.get(family);
        }

        // If this is a new column family, verify that the directory exists
        if (wl == null) {
          if (indexed) {
            indexFs.mkdirs(new Path(indexDir, Bytes.toString(family)));
          } else {
            fs.mkdirs(new Path(outputdir, Bytes.toString(family)));
          }
        }

        // If any of the HFiles for the column families has reached
        // maxsize, we need to roll all the writers
        if (wl != null && wl.written + length >= maxsize) {
          this.rollRequested = true;
        }

        // This can only happen once a row is finished though
        if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
          rollWriters();
        }

        // create a new HLog writer, if necessary
        if (wl == null || wl.writer == null) {
          wl = getNewWriter(family, conf, indexed);
        }

        // we now have the proper HLog writer. full steam ahead
        kv.updateLatestStamp(this.now);
        trt.includeTimestamp(kv);
        wl.writer.append(kv);
        wl.written += length;

        // Copy the row so we know when a row transition.
        this.previousRow = rowKey;
      }

      private void rollWriters() throws IOException {
        for (WriterLength wl : this.writers.values()) {
          if (wl.writer != null) {
            LOG.info("Writer=" + wl.writer.getPath()
                + ((wl.written == 0) ? "" : ", wrote=" + wl.written));
            close(wl.writer);
          }
          wl.writer = null;
          wl.written = 0;
        }
        this.rollRequested = false;
      }

      /*
       * Create a new HFile.Writer.
       * @param family
       * @return A WriterLength, containing a new HFile.Writer.
       * @throws IOException
       */
      private WriterLength getNewWriter(byte[] family, Configuration conf, boolean indexData)
          throws IOException {
        WriterLength wl = new WriterLength();

        Path familydir = null;

        String compression = compressionMap.get(family);
        compression = compression == null ? defaultCompression : compression;

        if (indexData) {
          familydir = new Path(indexDir, Bytes.toString(family));
          wl.writer =
              HFile.getWriterFactoryNoCache(conf)
                  .withPath(indexFs, StoreFile.getUniqueFile(indexFs, familydir))
                  .withBlockSize(blocksize).withCompression(compression)
                  .withComparator(KeyValue.KEY_COMPARATOR).withDataBlockEncoder(encoder)
                  .withChecksumType(Store.getChecksumType(conf))
                  .withBytesPerChecksum(Store.getBytesPerChecksum(conf)).create();
          this.writers.put(Bytes.toBytes(IndexMapReduceUtil.INDEX_DATA_DIR), wl);
        } else {
          familydir = new Path(outputdir, Bytes.toString(family));
          wl.writer =
              HFile.getWriterFactoryNoCache(conf)
                  .withPath(fs, StoreFile.getUniqueFile(fs, familydir)).withBlockSize(blocksize)
                  .withCompression(compression).withComparator(KeyValue.KEY_COMPARATOR)
                  .withDataBlockEncoder(encoder).withChecksumType(Store.getChecksumType(conf))
                  .withBytesPerChecksum(Store.getBytesPerChecksum(conf)).create();
          this.writers.put(family, wl);
        }

        return wl;
      }

      private void close(final HFile.Writer w) throws IOException {
        if (w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(context.getTaskAttemptID().toString()));
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
          w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(compactionExclude));
          w.appendFileInfo(StoreFile.TIMERANGE_KEY, WritableUtils.toByteArray(trt));
          w.close();
        }
      }

      public void close(TaskAttemptContext c) throws IOException, InterruptedException {
        for (WriterLength wl : this.writers.values()) {
          close(wl.writer);
        }
      }
    };

  }

  /**
   * Run inside the task to deserialize column family to compression algorithm map from the
   * configuration. Package-private for unit tests only.
   * @return a map from column family to the name of the configured compression algorithm
   */
  static Map<byte[], String> createFamilyCompressionMap(Configuration conf) {
    Map<byte[], String> compressionMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
    String compressionConf = conf.get(COMPRESSION_CONF_KEY, "");
    for (String familyConf : compressionConf.split("&")) {
      String[] familySplit = familyConf.split("=");
      if (familySplit.length != 2) {
        continue;
      }

      try {
        compressionMap.put(Bytes.toBytes(URLDecoder.decode(familySplit[0], "UTF-8")),
          URLDecoder.decode(familySplit[1], "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // will not happen with UTF-8 encoding
        throw new AssertionError(e);
      }
    }
    return compressionMap;
  }

  static class WriterLength {
    long written = 0;
    HFile.Writer writer = null;
  }
}
