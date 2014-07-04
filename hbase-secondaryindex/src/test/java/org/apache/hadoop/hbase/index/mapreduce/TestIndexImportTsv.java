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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.ParsedLine;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

@Category(LargeTests.class)
public class TestIndexImportTsv {

  @Test
  public void testTsvParserSpecParsing() {
    TsvParser parser;

    parser = new TsvParser("HBASE_ROW_KEY", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1,col1:scol2", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(2));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(2));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertFalse(parser.hasTimestamp());

    parser = new TsvParser("HBASE_ROW_KEY,col1:scol1,HBASE_TS_KEY,col1:scol2", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("scol1"), parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col1"), parser.getFamily(3));
    assertBytesEquals(Bytes.toBytes("scol2"), parser.getQualifier(3));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertTrue(parser.hasTimestamp());
    assertEquals(2, parser.getTimestampKeyColumnIndex());
  }

  @Test
  public void testTsvParser() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,col_b:qual,HBASE_ROW_KEY,col_d", "\t");
    assertBytesEquals(Bytes.toBytes("col_a"), parser.getFamily(0));
    assertBytesEquals(HConstants.EMPTY_BYTE_ARRAY, parser.getQualifier(0));
    assertBytesEquals(Bytes.toBytes("col_b"), parser.getFamily(1));
    assertBytesEquals(Bytes.toBytes("qual"), parser.getQualifier(1));
    assertNull(parser.getFamily(2));
    assertNull(parser.getQualifier(2));
    assertEquals(2, parser.getRowKeyColumnIndex());

    assertEquals(ImportTsv.TsvParser.DEFAULT_TIMESTAMP_COLUMN_INDEX,
      parser.getTimestampKeyColumnIndex());

    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c\tval_d");
    ParsedLine parsed = parser.parse(line, line.length);
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  @Test
  public void testTsvParserWithTimestamp() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,HBASE_TS_KEY,col_a,", "\t");
    assertNull(parser.getFamily(0));
    assertNull(parser.getQualifier(0));
    assertNull(parser.getFamily(1));
    assertNull(parser.getQualifier(1));
    assertBytesEquals(Bytes.toBytes("col_a"), parser.getFamily(2));
    assertBytesEquals(HConstants.EMPTY_BYTE_ARRAY, parser.getQualifier(2));
    assertEquals(0, parser.getRowKeyColumnIndex());
    assertEquals(1, parser.getTimestampKeyColumnIndex());

    byte[] line = Bytes.toBytes("rowkey\t1234\tval_a");
    ParsedLine parsed = parser.parse(line, line.length);
    assertEquals(1234l, parsed.getTimestamp(-1));
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  private void checkParsing(ParsedLine parsed, Iterable<String> expected) {
    ArrayList<String> parsedCols = new ArrayList<String>();
    for (int i = 0; i < parsed.getColumnCount(); i++) {
      parsedCols.add(Bytes.toString(parsed.getLineBytes(), parsed.getColumnOffset(i),
        parsed.getColumnLength(i)));
    }
    if (!Iterables.elementsEqual(parsedCols, expected)) {
      fail("Expected: " + Joiner.on(",").join(expected) + "\n" + "Got:"
          + Joiner.on(",").join(parsedCols));
    }
  }

  private void assertBytesEquals(byte[] a, byte[] b) {
    assertEquals(Bytes.toStringBinary(a), Bytes.toStringBinary(b));
  }

  /**
   * Test cases that throw BadTsvLineException
   */
  @Test(expected = BadTsvLineException.class)
  public void testTsvParserBadTsvLineExcessiveColumns() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("val_a\tval_b\tval_c");
    parser.parse(line, line.length);
  }

  @Test(expected = BadTsvLineException.class)
  public void testTsvParserBadTsvLineZeroColumn() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("");
    parser.parse(line, line.length);
  }

  @Test(expected = BadTsvLineException.class)
  public void testTsvParserBadTsvLineOnlyKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a", "\t");
    byte[] line = Bytes.toBytes("key_only");
    parser.parse(line, line.length);
  }

  @Test(expected = BadTsvLineException.class)
  public void testTsvParserBadTsvLineNoRowKey() throws BadTsvLineException {
    TsvParser parser = new TsvParser("col_a,HBASE_ROW_KEY", "\t");
    byte[] line = Bytes.toBytes("only_cola_data_and_no_row_key");
    parser.parse(line, line.length);
  }

  @Test(expected = BadTsvLineException.class)
  public void testTsvParserInvalidTimestamp() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,HBASE_TS_KEY,col_a,", "\t");
    assertEquals(1, parser.getTimestampKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\ttimestamp\tval_a");
    ParsedLine parsed = parser.parse(line, line.length);
    assertEquals(-1, parsed.getTimestamp(-1));
    checkParsing(parsed, Splitter.on("\t").split(Bytes.toString(line)));
  }

  @Test(expected = BadTsvLineException.class)
  public void testTsvParserNoTimestampValue() throws BadTsvLineException {
    TsvParser parser = new TsvParser("HBASE_ROW_KEY,col_a,HBASE_TS_KEY", "\t");
    assertEquals(2, parser.getTimestampKeyColumnIndex());
    byte[] line = Bytes.toBytes("rowkey\tval_a");
    parser.parse(line, line.length);
  }

  @Test
  public void testMROnTable() throws Exception {
    String TABLE_NAME = "testMROnTable";
    String FAMILY = "FAM";
    String INPUT_FILE = "InputFile.esv";

    // Prepare the arguments required for the test.
    String[] args =
        new String[] { "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b", TABLE_NAME, INPUT_FILE };

    doMROnTableTest(INPUT_FILE, FAMILY, TABLE_NAME, null, args, 1);
  }

  @Test
  public void testMROnTableWithTimestamp() throws Exception {
    String TABLE_NAME = "testMROnTableWithTimestamp";
    String FAMILY = "FAM";
    String INPUT_FILE = "InputFile1.csv";

    // Prepare the arguments required for the test.
    String[] args =
        new String[] {
            "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,HBASE_TS_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=,", TABLE_NAME, INPUT_FILE };

    String data = "KEY,1234,VALUE1,VALUE2\n";
    doMROnTableTest(INPUT_FILE, FAMILY, TABLE_NAME, data, args, 1);
  }

  private void doMROnTableTest(String inputFile, String family, String tableName, String data,
      String[] args, int valueMultiplier) throws Exception {

    // Cluster
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    Configuration conf = htu1.getConfiguration();
    conf.setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,true);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    htu1.startMiniCluster();
    htu1.startMiniMapReduceCluster();

    GenericOptionsParser opts = new GenericOptionsParser(htu1.getConfiguration(), args);
    conf = opts.getConfiguration();
    args = opts.getRemainingArgs();

    try {

      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream op = fs.create(new Path(inputFile), true);
      if (data == null) {
        data = "KEY\u001bVALUE1\u001bVALUE2\n";
      }
      op.write(Bytes.toBytes(data));
      op.close();

      final byte[] FAM = Bytes.toBytes(family);
      final byte[] TAB = Bytes.toBytes(tableName);

      Job job = IndexImportTsv.createSubmittableJob(conf, args);
      job.waitForCompletion(false);
      assertTrue(job.isSuccessful());
      String hfileOutputDir = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
      if (hfileOutputDir != null) {
        LoadIncrementalHFiles loader = new IndexLoadIncrementalHFile(htu1.getConfiguration());
        String[] args1 = { hfileOutputDir, tableName };
        assertEquals(0, loader.run(args1));
      }
      HTable table = new HTable(new Configuration(conf), TAB);
      HBaseAdmin admin = htu1.getHBaseAdmin();
      HTable indexTable = null;
      if(admin.isTableAvailable(IndexUtils.getIndexTableName(TAB))){
        indexTable = new HTable(conf,IndexUtils.getIndexTableName(TAB));
      }
      boolean verified = false;
      long pause = conf.getLong("hbase.client.pause", 5 * 1000);
      int numRetries = conf.getInt("hbase.client.retries.number", 5);
      for (int i = 0; i < numRetries; i++) {
        try {
          Scan scan = new Scan();
          // Scan entire family.
          scan.addFamily(FAM);
          ResultScanner resScanner = table.getScanner(scan);
          for (Result res : resScanner) {
            assertTrue(res.size() == 2);
            List<Cell> kvs = res.listCells();
            assertEquals(toU8Str(kvs.get(0).getRow()), toU8Str(Bytes.toBytes("KEY")));
            assertEquals(toU8Str(kvs.get(1).getRow()), toU8Str(Bytes.toBytes("KEY")));
            assertEquals(toU8Str(kvs.get(0).getValue()),
              toU8Str(Bytes.toBytes("VALUE" + valueMultiplier)));
            assertEquals(toU8Str(kvs.get(1).getValue()),
              toU8Str(Bytes.toBytes("VALUE" + 2 * valueMultiplier)));
            // Only one result set is expected, so let it loop.
          }
          if (indexTable != null) {
            scan = new Scan();
            resScanner = indexTable.getScanner(scan);
            int numKvs = 0;
            for (Result r : resScanner) {
              numKvs = +r.rawCells().length;
            }
            assertEquals(1, numKvs);
          }
          verified = true;
          break;
        } catch (NullPointerException e) {
          // If here, a cell was empty. Presume its because updates came in
          // after the scanner had been opened. Wait a while and retry.
        }
        try {
          Thread.sleep(pause);
        } catch (InterruptedException e) {
          // continue
        }
      }
      assertTrue(verified);
    } finally {
      htu1.shutdownMiniMapReduceCluster();
      htu1.shutdownMiniCluster();
    }
  }

  @Test
  public void testBulkOutputWithoutAnExistingTable() throws Exception {
    String TABLE_NAME = "testBulkOutputWithoutAnExistingTable";
    String FAMILY = "FAM";
    String INPUT_FILE = "InputFile2.esv";

    // Prepare the arguments required for the test.
    String[] args =
        new String[] { "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
            "-Dtable.columns.index=IDX1=>FAM:[A->String&10]",TABLE_NAME, INPUT_FILE };
    doMROnTableTest(INPUT_FILE, FAMILY, TABLE_NAME, null, args, 1);
  }

  @Test
  public void testIndexBulkLoad() throws Exception {
    String TABLE_NAME = "testIndexBulkLoad";
    String FAMILY = "FAM";
    String INPUT_FILE = "InputFile2.esv";

    // Prepare the arguments required for the test.
    String[] args =
        new String[] { "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
            "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=output",
            "-Dtable.columns.index=IDX1=>FAM:[A->String&10]",TABLE_NAME, INPUT_FILE };
    doMROnTableTest(INPUT_FILE, FAMILY, TABLE_NAME, null, args, 1);
  }

  public static String toU8Str(byte[] bytes) throws UnsupportedEncodingException {
    return Bytes.toString(bytes);
  }

}

