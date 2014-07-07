/**
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
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.AbstractHFileWriter;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.WriterLength;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexHFileOutputFormat extends FileOutputFormat<ImmutableBytesWritable, KeyValue> {

  static Log LOG = LogFactory.getLog(IndexHFileOutputFormat.class);
  private static final String DATABLOCK_ENCODING_CONF_KEY =
      "hbase.mapreduce.hfileoutputformat.datablock.encoding";
  TimeRangeTracker trt = new TimeRangeTracker();

  public static void configureIncrementalLoad(Job job, HTable table) throws IOException {
    HFileOutputFormat2.configureIncrementalLoad(job, table);
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
    // Invented config. Add to hbase-*.xml if other than default compression.
    final String defaultCompression =
        conf.get("hfile.compression", Compression.Algorithm.NONE.getName());
    final boolean compactionExclude =
        conf.getBoolean("hbase.mapreduce.hfileoutputformat.compaction.exclude", false);

    final boolean indexedTable = conf.getBoolean(IndexMapReduceUtil.IS_INDEXED_TABLE, false);

    final Path indexDir = new Path(outputdir, IndexMapReduceUtil.INDEX_DATA_DIR);

    final FileSystem indexFs = indexDir.getFileSystem(conf);

    if (indexedTable) {
      if (!indexFs.exists(indexDir)) {
        indexFs.mkdirs(indexDir);
      }
    }

    // TODO: read index table family details also separately.
    // create a map from column family to the compression algorithm
    final Map<byte[], Algorithm> compressionMap = HFileOutputFormat2.createFamilyCompressionMap(conf);
    final Map<byte[], BloomType> bloomTypeMap = HFileOutputFormat2.createFamilyBloomTypeMap(conf);
    final Map<byte[], Integer> blockSizeMap = HFileOutputFormat2.createFamilyBlockSizeMap(conf);

    final String dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_CONF_KEY);

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

        // TODO: what if user table also have same column family and qualifier?
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

        Compression.Algorithm compression = compressionMap.get(family);
        compression = compression == null ? Compression.Algorithm.NONE : compression;
        Integer blockSize = blockSizeMap.get(family);
        blockSize = blockSize == null ? HConstants.DEFAULT_BLOCKSIZE : blockSize;
        BloomType bloomType = bloomTypeMap.get(family);
        bloomType = bloomType==null?BloomType.NONE:bloomType;
        if (indexData) {
          familydir = new Path(indexDir, Bytes.toString(family));
        } else {
          familydir = new Path(outputdir, Bytes.toString(family));
        }
        Configuration tempConf = new Configuration(conf);
        tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
        HFileContextBuilder contextBuilder =
            new HFileContextBuilder()
                .withCompression(compression)
                .withChecksumType(HStore.getChecksumType(conf))
                .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf)).withBlockSize(blockSize);
        if (dataBlockEncodingStr != null) {
          contextBuilder.withDataBlockEncoding(DataBlockEncoding.valueOf(dataBlockEncodingStr));
        }
        HFileContext hFileContext = contextBuilder.build();

        wl.writer =
            new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
                .withOutputDir(familydir).withBloomType(bloomType)
                .withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext).build();
        if (indexData) {
          this.writers.put(Bytes.toBytes(IndexMapReduceUtil.INDEX_DATA_DIR), wl);
        } else {
          this.writers.put(family, wl);
        }
        return wl;
      }

      private void close(final StoreFile.Writer w) throws IOException {
        if (w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes.toBytes(System.currentTimeMillis()));
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(context.getTaskAttemptID().toString()));
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes.toBytes(true));
          w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(compactionExclude));
          w.appendTrackedTimestampsToMetadata();
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
}
