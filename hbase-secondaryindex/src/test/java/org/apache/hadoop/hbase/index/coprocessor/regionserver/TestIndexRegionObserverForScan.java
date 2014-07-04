/**
f * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hbase.index.coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIndexRegionObserverForScan {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin  = null; 

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    UTIL.startMiniCluster(1);
    admin = new IndexAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    IndexRegionObserver.setIndexedFlowUsed(false);
    IndexRegionObserver.setSeekpointAdded(false);
    IndexRegionObserver.setSeekPoints(null);
    IndexRegionObserver.setIsTestingEnabled(true);
  }

  @After
  public void tearDown() throws Exception {
    IndexRegionObserver.setIsTestingEnabled(false);
  }

  @Test(timeout = 180000)
  public void testScanIndexedColumnWithOnePutShouldRetreiveOneRowSuccessfully() throws IOException,
      KeeperException, InterruptedException {
    String userTableName = "testScanIndexedColumnWithOnePutShouldRetreiveOneRowSuccessfully";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);
    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);
    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    Assert.assertEquals("Should match for 1 row successfully ", 1, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public void testScanIndexedColumnWithOnePutAndSplitKeyatBorderShouldRetreiveOneRowSuccessfully()
      throws IOException, KeeperException, InterruptedException {
    String userTableName =
        "testScanIndexedColumnWithOnePutAndSplitKeyatBorderShouldRetreiveOneRowSuccessfully";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    byte[][] split =
        new byte[][] { "row1".getBytes(), "row21".getBytes(), "row41".getBytes(),
            "row61".getBytes(), "row81".getBytes(), "row101".getBytes(), "row121".getBytes(),
            "row141".getBytes(), };

    // create table with splits this will create 9 regions
    admin.createTable(ihtd, split);
    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);

    Put p1 = new Put("row01".getBytes());
    p1.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p1);

    Put p2 = new Put("row010".getBytes());
    p2.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p2);

    Put p3 = new Put("row001".getBytes());
    p3.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p3);

    validateCountOfMainTableIndIndexedTable(conf, userTableName, table);

    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    Assert.assertEquals("Should match for 1 row successfully ", 4, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public
      void
      testScanIndexedColumnWithOnePutAndSplitKeyAndStartKeyRegionEmptyShouldRetreiveOneRowSuccessfully()
          throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testScanIndexedColumnWithOnePutAndSplitKeyAndStartKeyRegionEmptyShouldRetreiveOneRowSuccessfully";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");

    byte[][] split = new byte[][] { "A".getBytes(), "B".getBytes(), "C".getBytes() };

    // create table with splits this will create 9 regions
    admin.createTable(ihtd, split);

    HTable table = new HTable(conf, userTableName);
    Put p = new Put("00row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);

    Put p1 = new Put("0row1".getBytes());
    p1.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p1);

    Put p2 = new Put("000row1".getBytes());
    p2.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p2);

    Put p3 = new Put("0000row1".getBytes());
    p3.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p3);

    // Check for verification of number of rows in main table and user table
    validateCountOfMainTableIndIndexedTable(conf, userTableName, table);

    // test put with the indexed column

    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    Assert.assertEquals("Should match for 1 row successfully ", 4, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public void
      testScanIndexedColumnWithOnePutAndSplitKeyHavingSpaceShouldRetreiveOneRowSuccessfully()
          throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanIndexedColumnWithOnePutAndSplitKeyHavingSpaceShouldRetreiveOneRowSuccessfully";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");

    byte[][] split =
        new byte[][] { " row1".getBytes(), "row21".getBytes(), "row41".getBytes(),
            "row61".getBytes(), "row81".getBytes(), "row101 ".getBytes(), "row121".getBytes(),
            "row141".getBytes(), };

    // create table with splits this will create 9 regions
    admin.createTable(ihtd, split);

    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);
    validateCountOfMainTableIndIndexedTable(conf, userTableName, table);
    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    Assert.assertEquals("Should match for 1 row successfully ", 1, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public void testScanIndexedColumnShouldNotRetreiveRowIfThereIsNoMatch() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanIndexedColumnShouldNotRetreiveRowIfThereIsNoMatch";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);

    Put p1 = new Put("row2".getBytes());
    p1.add("col".getBytes(), "ql".getBytes(), "Val1".getBytes());
    table.put(p1);
    int i = countNumberOfRowsWithFilter(userTableName, "unmatch", true, false, 0);
    Assert.assertEquals("Should not match any rows ", 0, i);
    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testScanIndexedColumnWith5PutsAnd3EqualPutValuesShouldRetreive3RowsSuccessfully()
      throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanIndexedColumnWith5PutsAnd3EqualPutValuesShouldRetreive3RowsSuccessfully";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    Put p1 = new Put("row1".getBytes());
    p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p1);

    Put p4 = new Put("row3".getBytes());
    p4.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p4);

    Put p5 = new Put("row2".getBytes());
    p5.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p5);

    Put p2 = new Put("row4".getBytes());
    p2.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p2);

    Put p3 = new Put("row5".getBytes());
    p3.add("col".getBytes(), "ql".getBytes(), "dogs".getBytes());
    table.put(p3);

    int i = countNumberOfRowsWithFilter(userTableName, "cat", true, false, 0);

    Assert.assertEquals("Should match for exactly 3 rows ", 3, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testPerformanceOfScanOnIndexedColumnShouldBeMoreWith1LakhRowsIfFlushIsNotMade()
      throws Exception {

    long withoutIndex = doPerformanceTest(false, "tableWithoutIndexAndWithoutFlush", false);
    long withIndex = doPerformanceTest(true, "tableWithIndexAndWithoutFlush", false);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    IndexRegionObserver.setSeekpointAdded(false);
    IndexRegionObserver.setIndexedFlowUsed(false);
    Assert.assertTrue(
      "Without flush time taken for Indexed scan should be less than without index ",
      withIndex < withoutIndex);

    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertFalse("Indexed table should not be used ",
      IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testShouldSuccessfullyReturn3RowsIfOnly3RowsMatchesIn1LakhRowsWithParallelPuts()
      throws Exception {
    String userTableName = "tableToCheck3Rows";
    doBulkParallelPuts(true, userTableName, false);
    int i = countNumberOfRowsWithFilter(userTableName, "cat", true, false, 0);
    Assert.assertEquals("Should match for exactly 3 rows in 1 lakh rows ", 3, i);
    Assert.assertTrue("Indexed table should  be used ", IndexRegionObserver.getSeekpointAdded());
  }

  @Test(timeout = 180000)
  public void testPerformanceOfScanOnIndexedColumnShouldBeMoreWith1LakhRowsIfFlushIsMade()
      throws Exception {
    long withIndex = doPerformanceTest(true, "tableWithIndexAndWithFlush", true);

    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    IndexRegionObserver.setSeekpointAdded(false);
    IndexRegionObserver.setIndexedFlowUsed(false);
    long withoutIndex = doPerformanceTest(false, "tableWithoutIndexAndWithFlush", true);
    Assert.assertTrue("With flush time taken for Indexed scan should be less than without index ",
      withIndex < withoutIndex);
    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertFalse("Indexed table should not be used ",
      IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testParallelScansShouldRetreiveRowsCorrectlyForIndexedColumn() throws Exception {
    String userTableName = "testParallelScansOnIndexedColumn";
    doParallelScanPuts(userTableName);
    ParallelScanThread p1 = new ParallelScanThread(userTableName, "cat", false, 0);
    p1.start();

    ParallelScanThread p2 = new ParallelScanThread(userTableName, "dog", false, 0);
    p2.start();

    ParallelScanThread p3 = new ParallelScanThread(userTableName, "pup", false, 0);
    p3.start();

    // wait for scan to complete
    p1.join();
    p2.join();
    p3.join();
    Assert.assertEquals("Should match for exactly 700 cats ", 700, p1.count);
    Assert.assertEquals("Should match for exactly 500 dogs ", 500, p2.count);
    Assert.assertEquals("Should match for exactly 300 pups ", 300, p3.count);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testParallelScansWithCacheShouldRetreiveRowsCorrectlyForIndexedColumn()
      throws Exception {
    String userTableName = "testParallelScansWithCacheOnIndexedColumn";
    doParallelScanPuts(userTableName);
    // In parallel scan setting the cache
    ParallelScanThread p1 = new ParallelScanThread(userTableName, "cat", true, 200);
    p1.start();

    ParallelScanThread p2 = new ParallelScanThread(userTableName, "dog", true, 200);
    p2.start();

    ParallelScanThread p3 = new ParallelScanThread(userTableName, "pup", true, 200);
    p3.start();

    // wait for scan to complete
    p1.join();
    p2.join();
    p3.join();
    Assert.assertEquals("Should match for exactly 700 cats ", 700, p1.count);
    Assert.assertEquals("Should match for exactly 500 dogs ", 500, p2.count);
    Assert.assertEquals("Should match for exactly 300 pups ", 300, p3.count);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testScanShouldBeSuccessfulEvenIfExceptionIsThrownFromPostScannerOpen()
      throws Exception {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanShouldBeSuccessfulEvenIfExceptionIsThrownFromPostScannerOpen";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "Val".getBytes());
    table.put(p);
    Scan s = new Scan();
    s.setLoadColumnFamiliesOnDemand(false);
    Filter filter = new FilterList();
    s.setFilter(filter);
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }

    Assert.assertEquals("Should match for 1 row successfully ", 1, i);
    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertFalse("Indexed table should not be used ",
      IndexRegionObserver.getIndexedFlowUsed());

  }

  private String doParallelScanPuts(String userTableName) throws IOException,
      ZooKeeperConnectionException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    List<Put> puts = new ArrayList<Put>();

    for (int i = 1; i <= 500; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    for (int i = 501; i <= 1000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    for (int i = 1001; i <= 1300; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "pup".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    for (int i = 1301; i <= 1500; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);
    return userTableName;
  }

  @Test(timeout = 180000)
  public void testScanShouldNotRetreiveRowsIfRowsArePresentOnlyInIndexedTableAndNotInMainTable()
      throws Exception {

    Configuration conf = UTIL.getConfiguration();
    final String userTableName = "testScanOnIndexedColumnForFalsePositve";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndex", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);

    List<Put> puts = new ArrayList<Put>();

    for (int i = 1; i <= 100; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 101; i <= 200; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 201; i <= 300; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "pup".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    // Doing one extra put explicitly into indexed table
    HTable indexTable = new HTable(conf, userTableName + Constants.INDEX_TABLE_SUFFIX);

    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
            .getRegionsOfTable(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));

    byte[] startRow = generateStartKey(regionsOfTable);

    Put p1 = new Put(startRow);
    p1.add("d".getBytes(), "ql".getBytes(), "idxCat".getBytes());
    indexTable.put(p1);

    int i = countNumberOfRowsWithFilter(userTableName, "idxCat", true, false, 0);
    Assert.assertEquals("Should not match any rows in main table ", 0, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public void testScanWithPutsAndCacheSetShouldRetreiveMatchingRows() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testcachedColumn";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    admin.createTable(ihtd);
    int i = singleIndexPutAndCache(conf, userTableName);
    Assert.assertEquals("Should match for exactly 5 rows ", 5, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertEquals("Remaining rows in cache should be 2  ", 2, IndexRegionObserver
        .getSeekpoints().size());

  }

  @Test(timeout = 180000)
  public void testScanMultipleIdxWithSameColFamilyAndDifferentQualifierShouldBeSuccessful()
      throws Exception {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMultIndexedSameColFamilyColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd1);
    IndexSpecification idx1 = new IndexSpecification("ScanMulIndex");
    idx1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(idx1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column
    Put p1 = new Put("row1".getBytes());
    p1.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p1.add("col1".getBytes(), "q2".getBytes(), "dog".getBytes());
    table.put(p1);

    Put p2 = new Put("row2".getBytes());
    p2.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col1".getBytes(), "q2".getBytes(), "cat".getBytes());
    table.put(p2);

    Put p3 = new Put("row3".getBytes());
    p3.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p3.add("col1".getBytes(), "q2".getBytes(), "dog".getBytes());
    table.put(p3);

    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q2
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col1".getBytes(), "q2".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);

    filterList.addFilter(filter2);
    s.setFilter(filterList);

    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }

    Assert.assertEquals("Should match for 2 rows in multiple index successfully ", 2, i);
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());

  }

  @Test(timeout = 180000)
  public void testScanMultipleIdxWithDifferentColFamilyShouldBeSuccessful() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMultIndexedDiffColFamilyColumn";
    putMulIndex(userTableName);
    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);
    HTable table = new HTable(conf, userTableName);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testScanMultipleIdxWithDifferentColFamilyAndCacheShouldBeSuccessful()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMultIndexedCacheDiffColFamilyColumn";
    putMulIndex(userTableName);
    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setCaching(4);
    s.setFilter(filterList);
    HTable table = new HTable(conf, userTableName);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
    Assert.assertEquals("Remaining rows in cache should be 1  ", 1, IndexRegionObserver
        .getSeekpoints().size());
  }

  @Test(timeout = 180000)
  public void
      testScanMultipleIdxWithDifferentFiltersShouldBeSuccessfulAndShouldNotGoWithIndexedFlow()
          throws Exception {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMultIndexedDiffFilters";
    putMulIndex(userTableName);
    HTable table = new HTable(conf, userTableName);
    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    Filter filter1 =
        new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator("row5".getBytes()));
    Filter filter2 = new FirstKeyOnlyFilter();
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);
    Assert.assertFalse("Seek points should not be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertFalse("Indexed table should not be used ",
      IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testScanWithIndexOn2ColumnsAndFiltersOn2ColumnsInReverseWayShouldBeSuccessful()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScan2Indexed2ReversedFilters";
    putMulIndex(userTableName);
    HTable table = new HTable(conf, userTableName);
    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);

    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public
      void
      testScanMultipleIdxWithDifferentColumnsInFiltersShouldBeSuccessfulAndShouldNotGoWithIndexedFlow()
          throws Exception {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "test11ScanWithMultIndexedDiff11Filters";

    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    ihtd.addFamily(hcd3);
    IndexSpecification idx1 = new IndexSpecification("ScanMulIndex");
    idx1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd2, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(idx1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());

    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);

    // test put with the multiple indexed column in diffrent column families
    Put p1 = new Put("row1".getBytes());
    p1.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p1.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p1);

    Put p2 = new Put("row2".getBytes());
    p2.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p2);

    Put p3 = new Put("row3".getBytes());
    p3.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p3.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p3);

    Put p4 = new Put("row4".getBytes());
    p4.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p4);

    Put p5 = new Put("row5".getBytes());
    p5.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p5.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p5);

    Put p6 = new Put("row6".getBytes());
    p6.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p6);

    Put p7 = new Put("row7".getBytes());
    p7.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p7.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p7);

    Put p9 = new Put("row8".getBytes());
    p9.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p9.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p9);

    Put p8 = new Put("row9".getBytes());
    p8.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p8.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p8);
    int i = 0;
    Scan s = new Scan();
    s.setLoadColumnFamiliesOnDemand(false);
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col3".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 1 rows in multiple index with diff column family successfully ", 1, i);
    Assert.assertTrue("Seek points should  be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());

    // Different values in column family should not retreive the rows.. Below
    // ensures the same
    Scan s1 = new Scan();
    s1.setLoadColumnFamiliesOnDemand(false);
    FilterList filterList1 = new FilterList();
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter11 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter11.setFilterIfMissing(true);
    SingleColumnValueFilter filter12 =
        new SingleColumnValueFilter("col3".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog1".getBytes());
    filter12.setFilterIfMissing(true);
    filterList1.addFilter(filter11);
    filterList1.addFilter(filter12);
    s1.setFilter(filterList1);
    i = 0;
    ResultScanner scanner1 = table.getScanner(s1);
    for (Result result : scanner1) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 0 rows in multiple index with diff column family successfully ", 0, i);
    Assert.assertTrue("Seek points should  be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());

  }

  @Test(timeout = 180000)
  public void testScanWith4IdxAnd2ColumnsInFiltersShouldBeSuccessful() throws Exception {
    HTable table = put4ColumnIndex();
    int i = 0;
    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    Filter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    Filter filter2 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);

    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);
    Assert.assertTrue("Seek points should be added ", IndexRegionObserver.getSeekpointAdded());
    Assert.assertTrue("Indexed table should be used ", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testScanShouldBeSuccessfulEvenIfUserRegionAndIndexRegionAreNotCollocated()
      throws Exception {

    // starting 3 RS
    UTIL.getMiniHBaseCluster().startRegionServer();
    UTIL.getMiniHBaseCluster().startRegionServer();
    Configuration conf = UTIL.getConfiguration();
    final String userTableName = "testCollocatedScansOnIndexedColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndex");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());

    byte[][] split =
        new byte[][] { "row1".getBytes(), "row21".getBytes(), "row41".getBytes(),
            "row61".getBytes(), "row81".getBytes(), "row101".getBytes(), "row121".getBytes(),
            "row141".getBytes(), };

    // create table with splits this will create 9 regions
    admin.createTable(ihtd, split);
    doPuts(conf, userTableName);

    HTable table = new HTable(conf, userTableName);
    // Now collocation between the RS's is done so put the rows

    UTIL.getMiniHBaseCluster().getMaster().balanceSwitch(false);
    // Now move the 3 rows that has the string "cat" to different RS

    collocateRowToDifferentRS(admin, table, "row1001");
    collocateRowToDifferentRS(admin, table, "row11004");
    collocateRowToDifferentRS(admin, table, "row11007");

    // Scan should be successful
    int i = countNumberOfRowsWithFilter(userTableName, "cat", true, false, 0);
    Assert.assertEquals("Should match for exactly 6000 rows ", 6000, i);

    UTIL.getMiniHBaseCluster().abortRegionServer(1);
    UTIL.getMiniHBaseCluster().abortRegionServer(2);
  }

  private void putMulIndex(String userTableName) throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();


    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    IndexSpecification idx1 = new IndexSpecification("ScanMulIndex");
    idx1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd2, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(idx1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());

    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);

    // test put with the multiple indexed column in diffrent column families
    Put p1 = new Put("row1".getBytes());
    p1.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p1.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p1);

    Put p2 = new Put("row2".getBytes());
    p2.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p2);

    Put p3 = new Put("row3".getBytes());
    p3.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p3.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p3);

    Put p4 = new Put("row4".getBytes());
    p4.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p4);

    Put p5 = new Put("row5".getBytes());
    p5.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p5.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p5);

    Put p6 = new Put("row6".getBytes());
    p6.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p6);

    Put p7 = new Put("row7".getBytes());
    p7.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p7.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p7);

    Put p8 = new Put("row8".getBytes());
    p8.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p8.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p8);

  }

  private void collocateRowToDifferentRS(HBaseAdmin admin, HTable table, String rowKey)
      throws IOException, UnknownRegionException, MasterNotRunningException,
      ZooKeeperConnectionException {

    HRegionInfo regionInfo = table.getRegionLocation(rowKey).getRegionInfo();
    int originServerNum = UTIL.getMiniHBaseCluster().getServerWith(regionInfo.getRegionName());
    int targetServerNum = 3 - 1 - originServerNum;
    HRegionServer targetServer = UTIL.getMiniHBaseCluster().getRegionServer(targetServerNum);
    admin.move(regionInfo.getEncodedNameAsBytes(),
      Bytes.toBytes(targetServer.getServerName().getServerName()));
    Threads.sleep(10000);
  }

  private void doPuts(Configuration conf, String userTableName) throws IOException {

    HTable table = new HTable(conf, userTableName);
    List<Put> puts = new ArrayList<Put>();

    for (int i = 1; i <= 2000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);
    puts = new ArrayList<Put>();

    for (int i = 2001; i <= 4000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 4001; i <= 6000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "pup".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 6001; i <= 8000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cats".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 8001; i <= 10000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "dogs".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 10001; i <= 12000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 12001; i <= 14000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "pup".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 14001; i <= 16000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
      puts.add(p1);
    }
    table.put(puts);

    puts = new ArrayList<Put>();
    for (int i = 16001; i <= 18000; i++) {
      Put p1 = new Put(("row" + i).getBytes());
      p1.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
      puts.add(p1);
    }
    table.put(puts);
    puts = new ArrayList<Put>();

  }

  class ParallelScanThread extends Thread {

    String filterString;
    String userTableName;
    int count;
    int cacheNumber;
    boolean iscached;

    ParallelScanThread(String userTableName, String filterString, boolean iscached, int cacheNumber)
        throws IOException {
      this.filterString = filterString;
      this.userTableName = userTableName;
      this.iscached = iscached;
      this.cacheNumber = cacheNumber;
    }

    public void run() {
      try {
        count =
            countNumberOfRowsWithFilter(userTableName, filterString, true, iscached, cacheNumber);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  class ParallelPutsValueThread extends Thread {

    int start;
    int end;
    String userTableName;
    Configuration conf;
    HTable table;
    String value;
    List<Put> puts;

    ParallelPutsValueThread(int start, Configuration conf, String userTableName, int end,
        String value) throws IOException {
      this.start = start;
      this.end = end;
      this.conf = conf;
      this.userTableName = userTableName;
      this.table = new HTable(this.conf, this.userTableName);
      this.value = value;
      puts = new ArrayList<Put>();
    }

    public void run() {

      for (int i = this.start; i <= this.end; i++) {
        Put p1 = new Put(("row" + i).getBytes());
        p1.add("col".getBytes(), "ql".getBytes(), value.getBytes());
        puts.add(p1);
      }
      try {
        this.table.put(puts);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void doBulkParallelPuts(boolean toIndex, final String userTableName, boolean toFlush)
      throws IOException, KeeperException, InterruptedException {
    final Configuration conf = UTIL.getConfiguration();
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    HTableDescriptor htd = null;
    if (toIndex) {
      htd =
          TestUtils.createIndexedHTableDescriptor(userTableName, "col", "ScanIndexf", "col", "ql");
    } else {
      htd = new HTableDescriptor(TableName.valueOf(userTableName));
      htd.addFamily(hcd);
    }
    admin.createTable(htd);

    ParallelPutsValueThread p1 = new ParallelPutsValueThread(1, conf, userTableName, 10000, "dogs");
    p1.start();

    Put a1 = new Put(("row" + 10001).getBytes());
    a1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    HTable table2 = new HTable(conf, userTableName);
    table2.put(a1);
    ParallelPutsValueThread p2 =
        new ParallelPutsValueThread(10002, conf, userTableName, 30000, "dogs");
    p2.start();
    Put a2 = new Put(("row" + 30001).getBytes());
    a2.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    HTable table4 = new HTable(conf, userTableName);
    table4.put(a2);

    ParallelPutsValueThread p3 =
        new ParallelPutsValueThread(30002, conf, userTableName, 60000, "dogs");
    p3.start();

    Put a3 = new Put(("row" + 60001).getBytes());
    a3.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    HTable table6 = new HTable(conf, userTableName);
    table6.put(a3);

    ParallelPutsValueThread p4 =
        new ParallelPutsValueThread(60002, conf, userTableName, 100000, "dogs");
    p4.start();

    p1.join();
    p2.join();
    p3.join();
    p4.join();

    if (toFlush) {
      if (toIndex) {
        admin.flush(userTableName + Constants.INDEX_TABLE_SUFFIX);
        admin.flush(userTableName);
      } else {
        admin.flush(userTableName);
      }
    }
  }

  private long doPerformanceTest(boolean toIndex, final String userTableName, boolean toFlush)
      throws IOException, KeeperException, InterruptedException {
    doBulkParallelPuts(toIndex, userTableName, toFlush);
    long before = System.currentTimeMillis();
    int i = countNumberOfRowsWithFilter(userTableName, "cat", toIndex, false, 0);
    long after = System.currentTimeMillis();
    return (after - before);

  }

  private int countNumberOfRowsWithFilter(String tableName, String filterVal, boolean isIndexed,
      boolean isCached, int cacheNumber) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, tableName);
    Scan s = new Scan();
    Filter filter = null;
    if (isIndexed) {
      filter =
          new SingleColumnValueFilter("col".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
              filterVal.getBytes());
    } else {
      filter =
          new SingleColumnValueFilter("col".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
              "cat".getBytes());
    }
    s.setFilter(filter);
    if (isCached) {
      s.setCaching(cacheNumber);
    }
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    return i;
  }

  private HTable put4ColumnIndex() throws IOException, ZooKeeperConnectionException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testcan4hMultIndexed2DiffFilters";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    HColumnDescriptor hcd4 = new HColumnDescriptor("col4");
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    ihtd.addFamily(hcd3);
    ihtd.addFamily(hcd4);
    IndexSpecification idx1 = new IndexSpecification("ScanMulIndex");
    idx1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd2, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd3, "ql", ValueType.String, 10);
    idx1.addIndexColumn(hcd4, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(idx1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());

    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);

    // test put with the multiple indexed column in diffrent column families
    Put p1 = new Put("row1".getBytes());
    p1.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p1.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p1.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p1.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p1);

    Put p2 = new Put("row2".getBytes());
    p2.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    p2.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p2);

    Put p3 = new Put("row3".getBytes());
    p3.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p3.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p3.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p3.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p3);

    Put p4 = new Put("row4".getBytes());
    p4.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p4);

    Put p5 = new Put("row5".getBytes());
    p5.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p5.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p5.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p5.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p5);

    Put p6 = new Put("row8".getBytes());
    p6.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p6.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p6);

    Put p8 = new Put("row6".getBytes());
    p8.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p8.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p8.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p8.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p8);

    Put p7 = new Put("row7".getBytes());
    p7.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p7.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p7.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    p7.add("col4".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p7);
    return table;
  }

  private byte[] generateStartKey(List<HRegionInfo> regionsOfTable) {
    byte[] startKey = regionsOfTable.get(0).getStartKey();

    byte[] startRow =
        new byte[startKey.length + Constants.DEF_MAX_INDEX_NAME_LENGTH + 10
            + "row999".getBytes().length];

    System.arraycopy(startKey, 0, startRow, 0, startKey.length);
    System.arraycopy("ScanIndex".getBytes(), 0, startRow, startKey.length, "ScanIndex".length());

    byte[] arr = new byte[18 - "ScanIndex".length()];
    byte e[] = new byte[10];

    System.arraycopy(arr, 0, startRow, startKey.length + "ScanIndex".length(), arr.length);
    System.arraycopy(e, 0, startRow, startKey.length + Constants.DEF_MAX_INDEX_NAME_LENGTH, 10);

    System.arraycopy("idxCat".getBytes(), 0, startRow, startKey.length
        + Constants.DEF_MAX_INDEX_NAME_LENGTH, "idxCat".getBytes().length);

    System.arraycopy("row99".getBytes(), 0, startRow, startKey.length
        + Constants.DEF_MAX_INDEX_NAME_LENGTH + 10, "row99".getBytes().length);

    System.out.println("constructed rowkey for indexed table " + Bytes.toString(startRow));
    return startRow;
  }

  private int singleIndexPutAndCache(Configuration conf, String userTableName) throws IOException {
    HTable table = new HTable(conf, userTableName);
    Put p1 = new Put("row1".getBytes());
    p1.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p1);

    Put p4 = new Put("row3".getBytes());
    p4.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p4);

    Put p5 = new Put("row2".getBytes());
    p5.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p5);

    Put p2 = new Put("row4".getBytes());
    p2.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p2);

    Put p3 = new Put("row5".getBytes());
    p3.add("col".getBytes(), "ql".getBytes(), "dogs".getBytes());
    table.put(p3);

    Put p6 = new Put("row6".getBytes());
    p6.add("col".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p6);

    Put p7 = new Put("row7".getBytes());
    p7.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p7);

    Put p8 = new Put("row8".getBytes());
    p8.add("col".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p8);

    int i = 0;
    Scan s = new Scan();
    Filter filter = null;
    filter =
        new SingleColumnValueFilter("col".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());

    s.setFilter(filter);
    s.setCaching(3);
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    return i;
  }

  private void validateCountOfMainTableIndIndexedTable(Configuration conf, String userTableName,
      HTable table) throws IOException {
    Scan s = new Scan();
    int j = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      j++;
    }

    HTable table1 = new HTable(conf, userTableName + Constants.INDEX_TABLE_SUFFIX);
    Scan s1 = new Scan();
    int k = 0;
    ResultScanner scanner1 = table1.getScanner(s1);
    for (Result result : scanner1) {
      k++;
    }

    Assert.assertEquals("COunt of rows in main tbale and indexed table shoudl be same ", j, k);
  }
  
  public static class MockedRegionObserver extends IndexRegionObserver {
    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e,
        Scan scan, RegionScanner s) {
      if (e.getEnvironment().getRegion().getTableDesc().getTableName().getNameAsString()
          .equals("testScanShouldBeSuccessfulEvenIfExceptionIsThrownFromPostScannerOpen")){
        throw new RuntimeException("Exception thrwn from postScannerOpen ");
      }
      return super.postScannerOpen(e, scan, s);
    }
  }
}
