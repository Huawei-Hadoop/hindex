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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexTsvImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  /** Timestamp for all inserted rows */
  private long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  private TsvParser parser;

  private String hfileOutPath;

  private boolean indexedTable;

  private List<IndexSpecification> indices = new ArrayList<IndexSpecification>();

  private byte[][] startKeys = null;

  public long getTs() {
    return ts;
  }

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser). Common
   * initialization that might be leveraged by a subsclass is done in <code>doSetup</code>. Hence a
   * subclass may choose to override this method and call <code>doSetup</code> as well before
   * handling it's own custom params.
   * @param context
   */
  @Override
  protected void setup(Context context) throws IOException {
    doSetup(context);

    Configuration conf = context.getConfiguration();

    parser = new TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY), separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
    String tableName = conf.get(TableInputFormat.INPUT_TABLE);
    HTable hTable = null;
    try {
      hTable = new HTable(conf, tableName);
      this.startKeys = hTable.getStartKeys();
      byte[] indexBytes = hTable.getTableDescriptor().getValue(Constants.INDEX_SPEC_KEY);
      if (indexBytes != null) {
        TableIndices tableIndices = new TableIndices();
        tableIndices.readFields(indexBytes);
        this.indices = tableIndices.getIndices();
      }
    } finally {
      if (hTable != null) hTable.close();
    }
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
    if (separator == null) {
      separator = ImportTsv.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }

    // Should never get 0 as we are setting this to a valid value in job configuration.
    ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, 0);

    skipBadLines = context.getConfiguration().getBoolean(ImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
    hfileOutPath = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
    indexedTable = conf.getBoolean(IndexMapReduceUtil.IS_INDEXED_TABLE, false);
  }

  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value, Context context) throws IOException {
    byte[] lineBytes = value.getBytes();

    try {
      ImportTsv.TsvParser.ParsedLine parsed = parser.parse(lineBytes, value.getLength());
      ImmutableBytesWritable rowKey =
          new ImmutableBytesWritable(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength());
      // Retrieve timestamp if exists
      ts = parsed.getTimestamp(ts);
      Configuration conf = context.getConfiguration();
      Put put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()) {
          continue;
        }
        KeyValue kv =
            new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
                parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
                parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes,
                parsed.getColumnOffset(i), parsed.getColumnLength(i));
        put.add(kv);
      }

      List<Put> indexPuts = new ArrayList<Put>();
      if (indexedTable && hfileOutPath != null) {
        try {
          // Genrate Index entry for the index put
          indexPuts = IndexMapReduceUtil.getIndexPut(put, indices, startKeys, conf);
        } catch (IOException e) {
          if (skipBadLines) {
            System.err.println("Bad line at offset: " + offset.get() + ":\n" + e.getMessage());
            incrementBadLineCount(1);
            return;
          } else {
            throw e;
          }
        }
        for (Put usrPut : indexPuts) {
          context.write(new ImmutableBytesWritable(usrPut.getRow()), usrPut);
        }
      }
      // Write user table put
      context.write(rowKey, put);

    } catch (TsvParser.BadTsvLineException badLine) {
      if (skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n" + badLine.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(badLine);
      }
    } catch (IllegalArgumentException e) {
      if (skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n" + e.getMessage());
        incrementBadLineCount(1);
        return;
      } else {
        throw new IOException(e);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}