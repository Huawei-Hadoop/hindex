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
package org.apache.hadoop.hbase.index.coprocessor.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.DoubleComparator;
import org.apache.hadoop.hbase.index.util.FloatComparator;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.index.util.IntComparator;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMultipleIndicesInScan {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    UTIL.startMiniCluster(1);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    admin = new IndexAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    IndexRegionObserver.setIsTestingEnabled(false);
  }

  @Before
  public void setUp() throws Exception {
    IndexRegionObserver.setIndexedFlowUsed(false);
    IndexRegionObserver.setSeekpointAdded(false);
    IndexRegionObserver.setSeekPoints(null);
    IndexRegionObserver.setIsTestingEnabled(true);
    IndexRegionObserver.addSeekPoints(null);
  }

  @Test(timeout = 180000)
  public void testAndOrCombinationWithMultipleIndices() throws IOException, KeeperException,
      InterruptedException {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSimpleScenarioForMultipleIndices";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    TableIndices indices = new TableIndices();
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2", "c1" }, "idx4");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testSimpleScenarioForMultipleIndices");

    putforIDX1(Bytes.toBytes("row0"), table);
    putforIDX1(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX3(Bytes.toBytes("row3"), table);

    putforIDX1(Bytes.toBytes("row4"), table);
    putforIDX2(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row4"), table);

    putforIDX1(Bytes.toBytes("row5"), table);
    putforIDX1(Bytes.toBytes("row6"), table);
    putforIDX2(Bytes.toBytes("row7"), table);
    putforIDX3(Bytes.toBytes("row8"), table);

    putforIDX1(Bytes.toBytes("row9"), table);
    putforIDX2(Bytes.toBytes("row9"), table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ONE);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "ele".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            "fan".getBytes());
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 2, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 2);
  }

  @Test(timeout = 180000)
  public void testReseekWhenSomeScannerAlreadyScannedGreaterValueThanSeekPoint()
      throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testReseekWhenSomeScannerAlreadyScannedGreaterValueThanSeekPoint";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2", "c1" }, "idx4");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table =
        new HTable(conf, "testReseekWhenSomeScannerAlreadyScannedGreaterValueThanSeekPoint");

    putforIDX1(Bytes.toBytes("row0"), table);
    putforIDX1(Bytes.toBytes("row3"), table);
    putforIDX1(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row6"), table);
    putforIDX3(Bytes.toBytes("row1"), table);
    putforIDX3(Bytes.toBytes("row3"), table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ONE);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "ele".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            "fan".getBytes());
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 2, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 2);
  }

  private void putforIDX3(byte[] row, HTable htable) throws IOException {
    Put p = new Put(row);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("bat"));
    htable.put(p);
  }

  private void putforIDX2(byte[] row, HTable htable) throws IOException {
    Put p = new Put(row);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("apple"));
    htable.put(p);
  }

  private void putforIDX1(byte[] row, HTable htable) throws IOException {
    Put p = new Put(row);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("cat"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c4"), Bytes.toBytes("dog"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c5"), Bytes.toBytes("ele"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c6"), Bytes.toBytes("fan"));
    htable.put(p);
  }

  private IndexSpecification createIndexSpecification(HColumnDescriptor hcd, ValueType type,
      int maxValueLength, String[] qualifiers, String name) {
    IndexSpecification index = new IndexSpecification(name.getBytes());
    for (String qualifier : qualifiers) {
      index.addIndexColumn(hcd, qualifier, type, maxValueLength);
    }
    return index;
  }

  @Test(timeout = 180000)
  public void testWhenAppliedFilterGetsNoScanScheme() throws Exception {

    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testWhenAppliedFilterGetsNoScanScheme";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2", "c1" }, "idx4");
    indices.addIndex(indexSpecification);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "ele".getBytes());
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testWhenAppliedFilterGetsNoScanScheme");

    putforIDX1(Bytes.toBytes("row1"), table);
    putforIDX1(Bytes.toBytes("row2"), table);
    putforIDX1(Bytes.toBytes("row3"), table);
    putforIDX1(Bytes.toBytes("row4"), table);

    Scan scan = new Scan();
    scan.setFilter(filter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertFalse("Index table should not be used", IndexRegionObserver.getIndexedFlowUsed());
    assertFalse("No seek points should get added from index flow",
      IndexRegionObserver.getSeekpointAdded());
    assertTrue(testRes.size() == 4);

  }

  @Test(timeout = 180000)
  public void testTheOROperation() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testTheOROperation";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2", "c1" }, "idx4");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testTheOROperation");

    putforIDX2(Bytes.toBytes("row1"), table);
    putforIDX3(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX2(Bytes.toBytes("row3"), table);
    putforIDX3(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row6"), table);
    putforIDX3(Bytes.toBytes("row7"), table);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should be used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("Index should fetch 7 seek points", 7, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Final result should have 7 rows.", 7, testRes.size());
  }

  @Test(timeout = 180000)
  public void testTheANDOpeartion() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testTheANDOpeartion";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testTheANDOpeartion");

    putforIDX2(Bytes.toBytes("row1"), table);
    putforIDX3(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX2(Bytes.toBytes("row3"), table);
    putforIDX3(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row6"), table);
    putforIDX3(Bytes.toBytes("row7"), table);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should be used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("Index should fetch 2 seek points", 2, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Final result should have 2 rows.", 2, testRes.size());
  }

  @Test(timeout = 180000)
  public void testAndOperationWithProperStartAndStopRow() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testAndOperationWithProperStartAndStopRow";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("5"));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 1);

  }

  @Test(timeout = 180000)
  public void testAndOperationWithSimilarValuePattern() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testAndOperationWithSimilarValuePattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("aaa"));
    table.put(p);
    p = new Put(Bytes.toBytes("row9"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("aaa1"));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("aaa3"));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("aaa4"));
    table.put(p);
    p = new Put(Bytes.toBytes("row7"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("aaa5"));
    table.put(p);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("aaa"));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 1);
    assertTrue("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("Index should fetch 2 seek points", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
  }

  @Test(timeout = 180000)
  public void testScanWithMutlipleIndicesOnTheSameColAndSimilarPattern() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMutlipleIndicesOnTheSameColAndSimilarPattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[][] val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row1"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row2"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row3"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog1"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row4"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1"), Bytes.toBytes("ant") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row5"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elefe"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row6"), val, table);

    table.flushCommits();
    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("cat"));
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("dog"));
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("elef"));
    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf1);
    masterFilter.addFilter(scvf2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 2);
  }

  @Test(timeout = 180000)
  public void testScanWithMutlipleIndicesWithGreaterthanEqualCondOnTheSameColAndSimilarPattern()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testScanWithMutlipleIndicesWithGreaterthanEqualCondOnTheSameColAndSimilarPattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[][] val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row1"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row2"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row3"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog1"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row4"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1"), Bytes.toBytes("ant") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row5"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elefe"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row6"), val, table);

    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row7"), val, table);

    table.flushCommits();
    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("cat"));
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("dog"));
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("elef"));
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.GREATER_OR_EQUAL,
            Bytes.toBytes("goat"));
    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf1);
    masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf3);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertEquals(testRes.size(), 3);
  }

  @Test(timeout = 180000)
  public void testScanWithMutlipleIndicesWithGreaterCondOnTheSameColAndSimilarPattern()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testScanWithMutlipleIndicesWithGreaterCondOnTheSameColAndSimilarPattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[][] val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row1"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row2"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row3"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog1"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row4"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1"), Bytes.toBytes("ant") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row5"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elefe"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row6"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row7"), val, table);

    table.flushCommits();
    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("cat"));
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("dog"));
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("elef"));
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.GREATER,
            Bytes.toBytes("goat"));

    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf1);
    masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf3);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertEquals(testRes.size(), 2);
  }

  @Test(timeout = 180000)
  public void testScanWithMutlipleIndicesWithLesserCondOnTheSameColAndSimilarPattern()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testScanWithMutlipleIndicesWithLesserCondOnTheSameColAndSimilarPattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[][] val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row1"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row2"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row3"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog1"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row4"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1"), Bytes.toBytes("ant") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row5"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elefe"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row6"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row7"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("gda") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row8"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goa") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row9"), val, table);

    table.flushCommits();
    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("cat"));
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("dog"));
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("elef"));
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.LESS,
            Bytes.toBytes("goat"));

    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf1);
    masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf3);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertEquals(testRes.size(), 2);
  }

  @Test(timeout = 180000)
  public void testScanWithMutlipleIndicesWithLesserEqualCondOnTheSameColAndSimilarPattern()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testScanWithMutlipleIndicesWithLesserEqualCondOnTheSameColAndSimilarPattern";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[][] val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row1"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("ele"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row2"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row3"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog1"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row4"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat1"), Bytes.toBytes("ant") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row5"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elefe"),
            Bytes.toBytes("goat1") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row6"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goat") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row7"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("gda") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row8"), val, table);
    val =
        new byte[][] { Bytes.toBytes("cat"), Bytes.toBytes("dog"), Bytes.toBytes("elef"),
            Bytes.toBytes("goa") };
    putsForIdx1WithDiffValues(Bytes.toBytes("row9"), val, table);

    table.flushCommits();
    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("cat"));
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("dog"));
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("elef"));
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.LESS_OR_EQUAL,
            Bytes.toBytes("goat"));

    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf1);
    masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf3);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertEquals(testRes.size(), 3);
  }

  private void putsForIdx1WithDiffValues(byte[] row, byte[][] valList, HTable table)
      throws IOException {
    Put p = new Put(row);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), (valList[0]));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c4"), valList[1]);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c5"), valList[2]);
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c6"), valList[3]);
    table.put(p);
  }

  @Test(timeout = 180000)
  public void testWhenSomePointsAreFetchedFromIndexButMainScanStillHasSomeFiltersToApply()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "MainScanStillHasSomeFiltersToApply";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());
    scvf.setFilterIfMissing(true);
    SingleColumnValueFilter scvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());
    scvf1.setFilterIfMissing(true);
    FilterList orFilter = new FilterList(Operator.MUST_PASS_ONE);
    orFilter.addFilter(scvf);
    orFilter.addFilter(scvf1);

    FilterList andFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    scvf2.setFilterIfMissing(true);
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    scvf3.setFilterIfMissing(true);
    SingleColumnValueFilter scvf4 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "ele".getBytes());
    scvf4.setFilterIfMissing(true);
    SingleColumnValueFilter scvf5 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            "fan".getBytes());
    scvf5.setFilterIfMissing(true);
    andFilter.addFilter(scvf5);
    andFilter.addFilter(scvf4);
    andFilter.addFilter(scvf3);
    andFilter.addFilter(scvf2);

    FilterList master = new FilterList(Operator.MUST_PASS_ALL);
    master.addFilter(andFilter);
    master.addFilter(orFilter);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "MainScanStillHasSomeFiltersToApply");

    putforIDX1(Bytes.toBytes("row0"), table);
    putforIDX1(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX3(Bytes.toBytes("row3"), table);

    putforIDX1(Bytes.toBytes("row4"), table);
    putforIDX2(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row4"), table);

    putforIDX1(Bytes.toBytes("row5"), table);
    putforIDX1(Bytes.toBytes("row6"), table);
    putforIDX2(Bytes.toBytes("row7"), table);
    putforIDX3(Bytes.toBytes("row8"), table);

    putforIDX1(Bytes.toBytes("row9"), table);
    putforIDX2(Bytes.toBytes("row9"), table);

    Scan scan = new Scan();
    scan.setFilter(master);
    // scan.setCaching(10);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should be used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("Index should fetch 6 seek points", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Final result should have 2 rows.", 2, testRes.size());
  }

  @Test(timeout = 180000)
  public void testWhenThereIsNoDataInIndexRegion() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testWhenThereIsNoDataInIndexRegion";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testWhenThereIsNoDataInIndexRegion");

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should be used.", IndexRegionObserver.getIndexedFlowUsed());
    assertFalse("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
  }

  @Test(timeout = 180000)
  public void testMultipleScansOnTheIndexRegion() throws Exception {
    final Configuration conf = UTIL.getConfiguration();
    String userTableName = "testMultipleScansOnTheIndexRegion";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2", "c1" }, "idx4");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testMultipleScansOnTheIndexRegion");

    putforIDX1(Bytes.toBytes("row0"), table);
    putforIDX1(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX3(Bytes.toBytes("row3"), table);

    putforIDX1(Bytes.toBytes("row4"), table);
    putforIDX2(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row4"), table);

    putforIDX1(Bytes.toBytes("row5"), table);
    putforIDX1(Bytes.toBytes("row6"), table);
    putforIDX2(Bytes.toBytes("row7"), table);
    putforIDX3(Bytes.toBytes("row8"), table);

    putforIDX1(Bytes.toBytes("row9"), table);
    putforIDX2(Bytes.toBytes("row9"), table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ONE);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "ele".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            "fan".getBytes());
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    Thread test = new testThread(masterFilter, "testMultipleScansOnTheIndexRegion");
    test.start();
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    test.join();
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 4, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 2);
  }

  private class testThread extends Thread {
    Filter filter = null;
    String tableName = null;

    public testThread(Filter filter, String tableName) {
      this.filter = filter;
      this.tableName = tableName;
    }

    @Override
    public synchronized void start() {
      try {
        HTable table = new HTable(UTIL.getConfiguration(), tableName);
        Scan scan = new Scan();
        scan.setFilter(filter);
        int i = 0;
        ResultScanner scanner = table.getScanner(scan);
        List<Result> testRes = new ArrayList<Result>();
        Result[] result = scanner.next(1);
        while (result != null && result.length > 0) {
          testRes.add(result[0]);
          i++;
          result = scanner.next(1);
        }
        assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
        assertTrue("Seekpoints should get added by index scanner",
          IndexRegionObserver.getSeekpointAdded());
        assertTrue("Overall result should have only 2 rows", testRes.size() == 2);
      } catch (IOException e) {
      }
    }
  }

  @Test(timeout = 180000)
  public void testFalsePositiveCases() throws Exception {
    final Configuration conf = UTIL.getConfiguration();
    String userTableName = "testFalsePositiveCases";
    HTableDescriptor ihtd = new HTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testFalsePositiveCases");

    HTable idx_table = new HTable(conf, "testFalsePositiveCases_idx");

    ByteArrayBuilder byteArray = new ByteArrayBuilder(33);
    byteArray.put(new byte[1]);
    byteArray.put(Bytes.toBytes("idx2"));
    byteArray.put(new byte[14]);
    byteArray.put(Bytes.toBytes("apple"));
    byteArray.put(new byte[5]);
    int offset = byteArray.position();
    byteArray.put(Bytes.toBytes("row1"));

    ByteArrayBuilder value = new ByteArrayBuilder(4);
    value.put(Bytes.toBytes((short) byteArray.array().length));
    value.put(Bytes.toBytes((short) offset));
    Put p = new Put(byteArray.array());
    p.add(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, value.array());
    idx_table.put(p);
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }

    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Overall result should have only 2 rows", 0, testRes.size());
  }

  @Test(timeout = 180000)
  public void testSingleLevelRangeScanForAND() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSingleLevelRangeScanForAND";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    // SingleColumnValueFilter scvfsub = new SingleColumnValueFilter("cf1".getBytes(),
    // "c1".getBytes(),
    // CompareOp.EQUAL, "5".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    // filterList1.addFilter(scvfsub);
    // filterList1.addFilter(scvf2sub);
    // filterList1.addFilter(scvf3sub);

    // SingleColumnValueFilter scvf = new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(),
    // CompareOp.GREATER_OR_EQUAL, "5".getBytes());
    // SingleColumnValueFilter scvf2 = new SingleColumnValueFilter("cf1".getBytes(),
    // "c2".getBytes(),
    // CompareOp.LESS_OR_EQUAL, "5".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());

    // masterFilter.addFilter(scvf);
    // masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(scvf3);
    // masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 2, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 2);
  }

  @Test(timeout = 180000)
  public void testSingleLevelRangeScanForOR() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSingleLevelRangeScanForOR";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    // SingleColumnValueFilter scvfsub = new SingleColumnValueFilter("cf1".getBytes(),
    // "c1".getBytes(),
    // CompareOp.EQUAL, "5".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    // filterList1.addFilter(scvfsub);
    // filterList1.addFilter(scvf2sub);
    // filterList1.addFilter(scvf3sub);

    // SingleColumnValueFilter scvf = new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(),
    // CompareOp.GREATER_OR_EQUAL, "5".getBytes());
    // SingleColumnValueFilter scvf2 = new SingleColumnValueFilter("cf1".getBytes(),
    // "c2".getBytes(),
    // CompareOp.LESS_OR_EQUAL, "5".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());

    // masterFilter.addFilter(scvf);
    // masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(scvf3);
    // masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 6 rows", testRes.size() == 6);
  }

  @Test(timeout = 180000)
  public void testEqualAndRangeCombinationWithMultipleIndices() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testEqualAndRangeCombinationWithMultipleIndices";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "4".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 1);
  }

  @Test(timeout = 180000)
  public void testEqualAndRangeCombinationWithMultipleIndicesPart2() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testEqualAndRangeCombinationWithMultipleIndicesPart2";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ALL);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "6".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 4, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 4);
  }

  @Test(timeout = 180000)
  public void testOREvaluatorWithMultipleOperatorsInEachLevel() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testOREvaluatorWithMultipleOperatorsInEachLevel";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "5".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    filterList1.addFilter(scvfsub);
    filterList1.addFilter(scvf2sub);
    filterList1.addFilter(scvf3sub);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "4".getBytes());

    masterFilter.addFilter(scvf);
    masterFilter.addFilter(scvf2);
    masterFilter.addFilter(scvf3);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 6 rows", testRes.size() == 6);
  }

  @Test(timeout = 180000)
  public void testIfAllScannersAreRangeInAllLevels() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIfAllScannersAreRangeInAllLevels";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ALL);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 6);
  }

  @Test(timeout = 180000)
  public void testANDWithORbranchesWhereEachBranchHavingAtleastOneFilterOtherThanSCVF()
      throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testANDWithORbranchesWhereEachBranchHavingAtleastOneFilterOtherThanSCVF";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList filterList2 = new FilterList(Operator.MUST_PASS_ONE);
    filterList2.addFilter(scvfsub);
    filterList2.addFilter(scvf2sub);
    filterList2.addFilter(scvf3sub);
    filterList2.addFilter(rowFilter);

    SingleColumnValueFilter scvfsub1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter2 =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList subFilterList = new FilterList(Operator.MUST_PASS_ONE);
    subFilterList.addFilter(scvfsub1);
    subFilterList.addFilter(scvf2sub2);
    subFilterList.addFilter(scvf3sub3);
    subFilterList.addFilter(rowFilter2);

    filterList1.addFilter(subFilterList);
    masterFilter.addFilter(filterList1);
    masterFilter.addFilter(filterList2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertFalse("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testORIfEachBranchHavingAtleastOneOtherFilterThanSCVF() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testORIfEachBranchHavingAtleastOneOtherFilterThanSCVF";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList filterList2 = new FilterList(Operator.MUST_PASS_ONE);
    filterList2.addFilter(scvfsub);
    filterList2.addFilter(scvf2sub);
    filterList2.addFilter(scvf3sub);
    filterList2.addFilter(rowFilter);

    SingleColumnValueFilter scvfsub1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter2 =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList subFilterList = new FilterList(Operator.MUST_PASS_ONE);
    subFilterList.addFilter(scvfsub1);
    subFilterList.addFilter(scvf2sub2);
    subFilterList.addFilter(scvf3sub3);
    subFilterList.addFilter(rowFilter2);

    filterList1.addFilter(subFilterList);
    masterFilter.addFilter(filterList1);
    masterFilter.addFilter(filterList2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertFalse("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testORBranchesInWhichOneBranchHavingOtherFiltersThanSCVF() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testORBranchesInWhichOneBranchHavingOtherFiltersThanSCVF";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList filterList2 = new FilterList(Operator.MUST_PASS_ONE);
    filterList2.addFilter(scvfsub);
    filterList2.addFilter(scvf2sub);
    filterList2.addFilter(scvf3sub);
    filterList2.addFilter(rowFilter);

    masterFilter.addFilter(filterList1);
    masterFilter.addFilter(filterList2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertFalse("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
  }

  @Test(timeout = 180000)
  public void testANDhavingORbranchWithOtherFilterThanSCVF() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testANDhavingORbranchWithOtherFilterThanSCVF";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "1".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    RowFilter rowFilter =
        new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row1")));
    FilterList filterList2 = new FilterList(Operator.MUST_PASS_ONE);
    filterList2.addFilter(scvfsub);
    filterList2.addFilter(scvf2sub);
    filterList2.addFilter(scvf3sub);
    filterList2.addFilter(rowFilter);

    masterFilter.addFilter(filterList1);
    masterFilter.addFilter(filterList2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 6);
  }

  @Test(timeout = 180000)
  public void testIfAllScannersAreRangeInAllLevelsPart2() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIfAllScannersAreRangeInAllLevelsPart2";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "1".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "4".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 1);
  }

  @Test(timeout = 180000)
  public void testIfAllScannersAreRangeInAllLevelsPart3() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIfAllScannersAreRangeInAllLevelsPart3";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ALL);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "4".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "1".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "2".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 1);
  }

  @Test(timeout = 180000)
  public void testOREvaluationFromMultipleLevels() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testOREvaluationFromMultipleLevels";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "1".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "4".getBytes());
    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);

    SingleColumnValueFilter scvfsub =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "6".getBytes());
    SingleColumnValueFilter scvf3sub =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "3".getBytes());

    masterFilter.addFilter(scvfsub);
    masterFilter.addFilter(scvf2sub);
    masterFilter.addFilter(scvf3sub);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 6, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 6);
  }

  private void rangePutForIdx2(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("1"));
    table.put(p);
    p = new Put(Bytes.toBytes("row9"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("2"));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("3"));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("4"));
    table.put(p);
    p = new Put(Bytes.toBytes("row7"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("5"));
    table.put(p);
    p = new Put(Bytes.toBytes("row15"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("6"));
    table.put(p);
  }

  private void rangePutForIdx3(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("1"));
    table.put(p);
    p = new Put(Bytes.toBytes("row9"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("2"));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("3"));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("4"));
    table.put(p);
    p = new Put(Bytes.toBytes("row7"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("5"));
    table.put(p);
    p = new Put(Bytes.toBytes("row15"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes("6"));
    table.put(p);
  }

  private void rangePutForIdx4(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("1"));
    table.put(p);
    p = new Put(Bytes.toBytes("row9"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("2"));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("3"));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("4"));
    table.put(p);
    p = new Put(Bytes.toBytes("row7"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("5"));
    table.put(p);
    p = new Put(Bytes.toBytes("row15"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("6"));
    table.put(p);
  }

  @Test(timeout = 180000)
  public void testCombinationOfLESSorGREATERwithEQUAL() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCombinationOfLESSorGREATERwithEQUAL";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10,
          new String[] { "c3", "c4", "c5", "c6" }, "idx1");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c3" }, "idx4");
    indices.addIndex(indexSpecification);

    // indexSpecification = createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2",
    // "c1" }, "idx4");
    // ihtd.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2(table);
    rangePutForIdx3(table);
    rangePutForIdx4(table);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);

    FilterList filterList1 = new FilterList(Operator.MUST_PASS_ONE);

    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "2".getBytes());
    SingleColumnValueFilter scvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    SingleColumnValueFilter scvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    SingleColumnValueFilter scvf4 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "5".getBytes());

    filterList1.addFilter(scvf);
    filterList1.addFilter(scvf2);
    filterList1.addFilter(scvf3);
    filterList1.addFilter(scvf4);
    masterFilter.addFilter(filterList1);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    System.out.println("************* result count......***********   " + testRes.size()
        + " ###### " + testRes);
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 4, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 4);
  }

  @Test(timeout = 180000)
  public void testIndexScanWithCaching() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIndexScanWithCaching";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx2");
    indices.addIndex(indexSpecification);
    indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c2" }, "idx3");
    indices.addIndex(indexSpecification);

    SingleColumnValueFilter filter =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "apple".getBytes());

    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "bat".getBytes());

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testIndexScanWithCaching");

    putforIDX2(Bytes.toBytes("row1"), table);
    putforIDX3(Bytes.toBytes("row1"), table);
    putforIDX2(Bytes.toBytes("row2"), table);
    putforIDX2(Bytes.toBytes("row3"), table);
    putforIDX3(Bytes.toBytes("row4"), table);
    putforIDX3(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row5"), table);
    putforIDX2(Bytes.toBytes("row6"), table);
    putforIDX3(Bytes.toBytes("row7"), table);

    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    scan.setCaching(10);
    int i = 0;
    ResultScanner scanner = table.getScanner(scan);
    int nextCalled = 0;
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(10);
    nextCalled++;
    while (result != null && result.length > 1) {
      for (int j = 0; j < result.length; j++) {
        testRes.add(result[j]);
      }
      i++;
      result = scanner.next(10);
      nextCalled++;
    }

    assertTrue("Index flow should be used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Index should fetch some seek points.",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("Index should fetch 7 seek points", 7, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Final result should have 7 rows.", 7, testRes.size());
    assertEquals("All rows should be fetched in single next call.", 2, nextCalled);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegtiveIntValueWithEqualCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testOtherDataTypes";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Int, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithInteger(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes(-4));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 1);
    assertTrue(testRes.toString().contains("row3"));
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeIntValueWithLessCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeIntValueWithLessCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Int, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithInteger(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            (Bytes.toBytes(-4)));

    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 2);
  }

  @Test(timeout = 180000)
  public void testShouldRetriveNegativeIntValueWithGreaterCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetriveNegativeIntValueWithGreaterCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Int, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithInteger(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            new IntComparator(Bytes.toBytes(-6)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 5);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeIntValue() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeIntValue";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithInteger(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            new IntComparator(Bytes.toBytes(-6)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 5);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeFloatValueWithGreaterCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeFloatValueWithGreaterCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Float, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithFloat(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            new FloatComparator(Bytes.toBytes(-5f)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 4);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeFloatValueWithLessCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeFloatValueWithLessCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Float, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithFloat(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            new FloatComparator(Bytes.toBytes(-5f)));

    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 2);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeFloatValueWithEqualsCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeFloatValueWithEqualsCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Float, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithFloat(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            new FloatComparator(Bytes.toBytes(-5.3f)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 1);
  }

  @Test(timeout = 180000)
  public void testShouldRetrieveNegativeDoubleValueWithLesserThanEqualsCondition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeDoubleValueWithLesserThanEqualsCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Double, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithDouble(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            new DoubleComparator(Bytes.toBytes(-5.3d)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 2);
  }

  @Test//(timeout = 180000)
  public void testShouldRetrieveNegativeDoubleValueWithGreaterThanEqualsCondition()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testShouldRetrieveNegativeDoubleValueWithGreaterThanEqualsCondition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.Double, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    rangePutForIdx2WithDouble(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            new DoubleComparator(Bytes.toBytes(-5.3d)));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      result = scanner.next(1);
    }
    assertTrue(testRes.size() == 5);
  }

  @Test(timeout = 180000)
  public void testCachingWithValuesDistributedAmongMulitpleRegions() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCachingWithValuesDistributedAmongMulitpleRegions";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    byte[][] split =
        new byte[][] { Bytes.toBytes("row1"), Bytes.toBytes("row2"), Bytes.toBytes("row3"),
            Bytes.toBytes("row4") };
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd, split);
    HTable table = new HTable(conf, userTableName);
    insert100Rows(table);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("5"));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setCaching(5);
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    int i = 0;
    while (result != null && result.length > 0) {
      System.out.println(Bytes.toString(result[0].getRow()));
      testRes.add(result[0]);
      result = scanner.next(1);
      i++;
    }
    assertEquals(8, i);
  }

  @Test(timeout = 180000)
  public void testCachingWithValuesWhereSomeRegionsDontHaveAnyData() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCachingWithValuesWhereSomeRegionsDontHaveAnyData";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    byte[][] split =
        new byte[][] { Bytes.toBytes("row1"), Bytes.toBytes("row3"), Bytes.toBytes("row5"),
            Bytes.toBytes("row7") };
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd, split);
    HTable table = new HTable(conf, userTableName);
    for (int i = 0; i < 10; i++) {
      if (i > 4 && i < 8) {
        continue;
      }
      Put p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("5"));
      table.put(p);
    }

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("5"));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setCaching(5);
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    int i = 0;
    while (result != null && result.length > 0) {
      System.out.println(Bytes.toString(result[0].getRow()));
      testRes.add(result[0]);
      result = scanner.next(1);
      i++;
    }
    assertEquals(7, i);
  }

  @Test(timeout = 180000)
  public void testCachingWithLessNumberOfRowsThanCaching() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCachingWithLessNumberOfRowsThanCaching";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    IndexSpecification indexSpecification =
        createIndexSpecification(hcd, ValueType.String, 10, new String[] { "c1" }, "idx1");
    indices.addIndex(indexSpecification);
    byte[][] split =
        new byte[][] { Bytes.toBytes("row1"), Bytes.toBytes("row3"), Bytes.toBytes("row5"),
            Bytes.toBytes("row7") };
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd, split);
    HTable table = new HTable(conf, userTableName);
    for (int i = 0; i < 10; i++) {
      if (i > 4 && i < 8) {
        continue;
      }
      Put p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("5"));
      table.put(p);
    }

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes("5"));
    masterFilter.addFilter(scvf);
    Scan scan = new Scan();
    scan.setCaching(10);
    scan.setFilter(masterFilter);
    ResultScanner scanner = table.getScanner(scan);
    List<Result> testRes = new ArrayList<Result>();
    Result[] result = scanner.next(1);
    int i = 0;
    while (result != null && result.length > 0) {
      System.out.println(Bytes.toString(result[0].getRow()));
      testRes.add(result[0]);
      result = scanner.next(1);
      i++;
    }
    assertEquals(7, i);
  }

  private void insert100Rows(HTable table) throws IOException {
    for (int i = 0; i < 8; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes("5"));

      table.put(p);
    }
  }

  private void rangePutForIdx2WithInteger(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(1));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(2));
    table.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(3));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-4));
    table.put(p);
    p = new Put(Bytes.toBytes("row4"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-5));
    table.put(p);
    p = new Put(Bytes.toBytes("row5"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-6));
    table.put(p);
  }

  private void rangePutForIdx2WithFloat(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(1.5f));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(2.89f));
    table.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(3.9f));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-4.7f));
    table.put(p);
    p = new Put(Bytes.toBytes("row4"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-5.3f));
    table.put(p);
    p = new Put(Bytes.toBytes("row5"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-6.456f));
    table.put(p);
  }

  private void rangePutForIdx2WithDouble(HTable table) throws IOException {
    Put p = new Put(Bytes.toBytes("row0"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(1.5d));
    table.put(p);
    p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(2.89d));
    table.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(3.9d));
    table.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-4.7d));
    table.put(p);
    p = new Put(Bytes.toBytes("row4"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-5.3d));
    table.put(p);
    p = new Put(Bytes.toBytes("row5"));
    p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(-6.456d));
    table.put(p);
  }

  @Test(timeout = 180000)
  public void testComplexRangeScan() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String tableName = "testComplexRangeScan";
    IndexSpecification spec1 = new IndexSpecification("idx1");
    IndexSpecification spec2 = new IndexSpecification("idx2");
    IndexSpecification spec3 = new IndexSpecification("idx3");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    // HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    spec1.addIndexColumn(hcd, "detail", ValueType.String, 10);
    spec2.addIndexColumn(hcd, "info", ValueType.String, 10);
    spec3.addIndexColumn(hcd, "value", ValueType.String, 10);
    htd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(spec1);
    indices.addIndex(spec2);
    indices.addIndex(spec3);
    String[] splitkeys = new String[9];

    for (int i = 100, j = 0; i <= 900; i += 100, j++) {
      splitkeys[j] = new Integer(i).toString();
    }
    htd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(htd, Bytes.toByteArrays(splitkeys));
    String rowname = "row";
    String startrow = "";
    int keys = 0;
    List<Put> put = new ArrayList<Put>();
    for (int i = 1, j = 999; i < 1000; i++, j--) {
      if (i % 100 == 0) {
        startrow = splitkeys[keys++];
      }
      Put p = new Put(Bytes.toBytes(startrow + rowname + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("detail"), Bytes.toBytes(new Integer(i).toString()));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(new Integer(j).toString()));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("value"),
        Bytes.toBytes(new Integer(i % 100).toString()));
      System.out.println(p);
      put.add(p);
    }
    HTable table = new HTable(conf, tableName);
    table.put(put);

    Scan s = new Scan();
    s.setCacheBlocks(true);
    FilterList master = new FilterList(Operator.MUST_PASS_ONE);
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf".getBytes(), "detail".getBytes(), CompareOp.LESS_OR_EQUAL,
            "6".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("cf".getBytes(), "info".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "992".getBytes());
    filter2.setFilterIfMissing(true);
    SingleColumnValueFilter filter3 =
        new SingleColumnValueFilter("cf".getBytes(), "value".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    filter3.setFilterIfMissing(true);
    master.addFilter(filter1);
    master.addFilter(filter2);
    master.addFilter(filter3);
    s.setFilter(master);
    // scanOperation(s, conf, tableName);
    assertEquals("data consistency is missed ", 563, scanOperation(s, conf, tableName));
    System.out.println("Done ************");
    s = new Scan();
    s.setFilter(master);
    s.setCaching(5);
    // scanOperation(s, conf, tableName);
    assertEquals("data consistency is missed ", 563, scanOperation(s, conf, tableName));
  }

  @Test(timeout = 180000)
  public void testComplexRangeScanWithAnd() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String tableName = "RangeScanMetrix_2_new_id";
    IndexSpecification spec1 = new IndexSpecification("idx1");
    IndexSpecification spec2 = new IndexSpecification("idx2");
    IndexSpecification spec3 = new IndexSpecification("idx3");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    // HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    spec1.addIndexColumn(hcd, "detail", ValueType.String, 10);
    spec2.addIndexColumn(hcd, "info", ValueType.String, 10);
    spec3.addIndexColumn(hcd, "value", ValueType.String, 10);
    htd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(spec1);
    indices.addIndex(spec2);
    indices.addIndex(spec3);
    String[] splitkeys = new String[9];

    for (int i = 100, j = 0; i <= 900; i += 100, j++) {
      splitkeys[j] = new Integer(i).toString();
    }
    htd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(htd, Bytes.toByteArrays(splitkeys));
    String rowname = "row";
    String startrow = "";
    int keys = 0;
    List<Put> put = new ArrayList<Put>();
    for (int i = 1, j = 999; i < 1000; i++, j--) {
      if (i % 100 == 0) {
        startrow = splitkeys[keys++];
      }
      Put p = new Put(Bytes.toBytes(startrow + rowname + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("detail"), Bytes.toBytes(new Integer(i).toString()));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("info"), Bytes.toBytes(new Integer(j).toString()));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("value"),
        Bytes.toBytes(new Integer(i % 100).toString()));
      System.out.println(p);
      put.add(p);
    }
    HTable table = new HTable(conf, tableName);
    table.put(put);

    Scan s = new Scan();
    s.setCacheBlocks(true);
    s.setCaching(1);
    FilterList master = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf".getBytes(), "detail".getBytes(), CompareOp.LESS_OR_EQUAL,
            "65".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("cf".getBytes(), "info".getBytes(), CompareOp.GREATER,
            "900".getBytes());
    filter2.setFilterIfMissing(true);
    SingleColumnValueFilter filter3 =
        new SingleColumnValueFilter("cf".getBytes(), "value".getBytes(),
            CompareOp.GREATER_OR_EQUAL, "5".getBytes());
    filter3.setFilterIfMissing(true);
    master.addFilter(filter1);
    master.addFilter(filter2);
    master.addFilter(filter3);
    s.setFilter(master);
    // scanOperation(s, conf, tableName);
    assertEquals("data consistency is missed ", 18, scanOperation(s, conf, tableName));
    System.out.println("Done ************");
    s = new Scan();
    s.setFilter(master);
    s.setCaching(5);
    // scanOperation(s, conf, tableName);
    assertEquals("data consistency is missed ", 18, scanOperation(s, conf, tableName));
  }

  @Test(timeout = 180000)
  public void testVerifyTheStopRowIsCorrectInCaseOfGreaterOperatorsInSCVF() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String tableName = "testDeleteIncosistent";
    IndexSpecification spec1 = new IndexSpecification("idx1");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    // HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    spec1.addIndexColumn(hcd, "detail", ValueType.String, 10);
    htd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(spec1);
    htd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(htd);
    HTable table = new HTable(conf, "testDeleteIncosistent");
    HTable table2 = new HTable(conf, "testDeleteIncosistent_idx");
    Put p = new Put(Bytes.toBytes("row5"));
    p.add("cf".getBytes(), "detail".getBytes(), "5".getBytes());
    table2.put(IndexUtils.prepareIndexPut(p, spec1, HConstants.EMPTY_START_ROW));
    p = new Put(Bytes.toBytes("row6"));
    p.add("cf".getBytes(), "detail".getBytes(), "6".getBytes());
    table.put(p);
    Scan s = new Scan();
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("cf".getBytes(), "detail".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    s.setFilter(filter1);
    ResultScanner scanner = table.getScanner(s);
    int i = 0;
    for (Result result : scanner) {
      i++;
    }
    assertEquals(1, i);
  }

  private int scanOperation(Scan s, Configuration conf, String tableName) {
    ResultScanner scanner = null;
    int i = 0;
    try {
      HTable table = new HTable(conf, tableName);
      scanner = table.getScanner(s);

      for (Result result : scanner) {
        System.out.println(Bytes.toString(result.getRow()));
        i++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    System.out.println("******* Return value " + i);
    return i;
  }

}
