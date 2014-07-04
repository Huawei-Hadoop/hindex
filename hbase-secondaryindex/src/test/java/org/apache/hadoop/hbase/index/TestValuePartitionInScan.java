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
package org.apache.hadoop.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.client.EqualsExpression;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.client.IndexUtils;
import org.apache.hadoop.hbase.index.client.SingleIndexExpression;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.filter.SingleColumnValuePartitionFilter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestValuePartitionInScan {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.hregion.scan.loadColumnFamiliesOnDemand", false);
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
  public void testSeparatorPartition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSeparatorPartition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SeparatorPartition("_", 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 200);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testSeparatorPartition");
    byte[] value1 = "2ndFloor_solitaire_huawei_bangalore_karnataka".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(),
      "7thFloor_solitaire_huawei_bangalore_karnataka".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "rrr_sss_hhh_bangalore_karnataka".getBytes());
    table.put(p);

    Scan scan = new Scan();
    scan.setCaching(1);
    scan.setFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "huawei".getBytes(), vp));
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
    assertEquals("Overall result should have only 2 rows", 2, testRes.size());
  }

  @Test(timeout = 180000)
  public void testSpatialPartition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSpatialPartition";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SpatialPartition(2, 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 200);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, "testSpatialPartition");
    byte[] value1 = "helloworld".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "spatial".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partition".getBytes());
    table.put(p);

    Scan scan = new Scan();
    scan.setFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "rti".getBytes(), vp));
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
    assertEquals("It should get 1 seek point from index scanner.", 1, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertEquals("Overall result should have only 1 rows", 1, testRes.size());
  }

  @Test//(timeout = 180000)
  public void testSpatialPartitionIfMulitplePartsOfValueAreIndexedByDifferentIndicesOnSameColumn()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testSpatialPartitionIfMulitplePartsOfValueAreIndexedByDifferentIndicesOnSameColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    ValuePartition vp = new SpatialPartition(2, 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 10);
    indices.addIndex(iSpec);
    ValuePartition vp2 = new SpatialPartition(5, 2);
    iSpec = new IndexSpecification("idx2");
    iSpec.addIndexColumn(hcd, "cq", vp2, ValueType.String, 10);
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[] value1 = "helloworldmultiple".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "spatialmultiple".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partitionmultiple".getBytes());
    table.put(p);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "rti".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "ti".getBytes(), vp2));
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
    assertEquals("Overall result should have only 1 rows", 1, testRes.size());

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.LESS, "rti".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER, "ti".getBytes(), vp2));
    scan = new Scan();
    scan.setFilter(masterFilter);
    i = 0;
    scanner = table.getScanner(scan);
    testRes = new ArrayList<Result>();
    result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 3, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 2 rows", testRes.size() == 2);

  }

  @Test(timeout = 180000)
  public void
      testSeparatorPartitionIfMulitplePartsOfValueAreIndexedByDifferentIndicesOnSameColumn()
          throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName =
        "testSeparatorPartitionIfMulitplePartsOfValueAreIndexedByDifferentIndicesOnSameColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    TableIndices indices = new TableIndices();
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SeparatorPartition("--", 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 200);
    indices.addIndex(iSpec);
    ValuePartition vp2 = new SeparatorPartition("--", 2);
    iSpec = new IndexSpecification("idx2");
    iSpec.addIndexColumn(hcd, "cq", vp2, ValueType.String, 200);
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[] value1 = "hello--world--multiple--1".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "spatial--partition--multiple".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partition--by--separator--multiple".getBytes());
    table.put(p);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "by".getBytes(), vp2));
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
    assertTrue("Overall result should have only 1 rows", testRes.size() == 2);

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "person".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.LESS, "multiple".getBytes(), vp2));
    scan = new Scan();
    scan.setFilter(masterFilter);
    i = 0;
    scanner = table.getScanner(scan);
    testRes = new ArrayList<Result>();
    result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 3, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 1 rows", testRes.size() == 1);

  }

  @Test(timeout = 180000)
  public void testCombinationOfPartitionFiltersWithSCVF() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCombinationOfPartitionFiltersWithSCVF";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    TableIndices indices = new TableIndices();
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SeparatorPartition("--", 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 200);
    indices.addIndex(iSpec);
    ValuePartition vp2 = new SeparatorPartition("--", 2);
    iSpec = new IndexSpecification("idx2");
    iSpec.addIndexColumn(hcd, "cq", vp2, ValueType.String, 200);
    indices.addIndex(iSpec);
    iSpec = new IndexSpecification("idx3");
    iSpec.addIndexColumn(hcd, "cq", ValueType.String, 200);
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[] value1 = "hello--world--multiple--1".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "spatial--partition--multiple".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partition--by--separator--multiple".getBytes());
    table.put(p);
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "by".getBytes(), vp2));
    masterFilter.addFilter(new SingleColumnValueFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "spatial--partition--multiple".getBytes()));

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
    assertTrue("Overall result should have only 1 rows", testRes.size() == 1);

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    masterFilter.addFilter(new SingleColumnValueFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "partition--by--separator--multiple".getBytes()));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "person".getBytes(), vp));
    masterFilter.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.LESS, "multiple".getBytes(), vp2));
    scan = new Scan();
    scan.setFilter(masterFilter);
    i = 0;
    scanner = table.getScanner(scan);
    testRes = new ArrayList<Result>();
    result = scanner.next(1);
    while (result != null && result.length > 0) {
      testRes.add(result[0]);
      i++;
      result = scanner.next(1);
    }
    assertTrue("Index flow should get used.", IndexRegionObserver.getIndexedFlowUsed());
    assertTrue("Seekpoints should get added by index scanner",
      IndexRegionObserver.getSeekpointAdded());
    assertEquals("It should get two seek points from index scanner.", 3, IndexRegionObserver
        .getMultipleSeekPoints().size());
    assertTrue("Overall result should have only 1 rows", testRes.size() == 2);

  }

  @Test(timeout = 180000)
  public void testCombinationOfPartitionFiltersWithSCVFPart2() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCombinationOfPartitionFiltersWithSCVFPart2";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    TableIndices indices = new TableIndices();
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SeparatorPartition("--", 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 100);
    iSpec.addIndexColumn(hcd, "cq1", ValueType.String, 100);
    indices.addIndex(iSpec);
    ValuePartition vp2 = new SeparatorPartition("--", 2);
    iSpec = new IndexSpecification("idx2");
    iSpec.addIndexColumn(hcd, "cq", vp2, ValueType.String, 100);
    indices.addIndex(iSpec);
    iSpec = new IndexSpecification("idx3");
    iSpec.addIndexColumn(hcd, "cq1", ValueType.String, 100);
    indices.addIndex(iSpec);
    iSpec = new IndexSpecification("idx4");
    iSpec.addIndexColumn(hcd, "cq", ValueType.String, 100);
    iSpec.addIndexColumn(hcd, "cq1", ValueType.String, 100);
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[] value1 = "hello--world--multiple--1".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "spatial--partition--multiple".getBytes());
    p.add("cf1".getBytes(), "cq1".getBytes(), "spatialPartition".getBytes());
    table.put(p);
    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partition--by--multiple--multiple".getBytes());
    p.add("cf1".getBytes(), "cq1".getBytes(), "partitionValue".getBytes());
    table.put(p);
    p = new Put("row4".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "partition--multiple--multiple--multiple".getBytes());
    p.add("cf1".getBytes(), "cq1".getBytes(), "multiple".getBytes());
    table.put(p);
    p = new Put("row5".getBytes());
    p.add("cf1".getBytes(), "cq1".getBytes(), "abcd".getBytes());
    table.put(p);
    p = new Put("row6".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "1234".getBytes());
    table.put(p);

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    filter1.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp));
    filter1.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "by".getBytes(), vp2));
    filter1.addFilter(new SingleColumnValueFilter(hcd.getName(), "cq".getBytes(), CompareOp.EQUAL,
        "partition--multiple--multiple--multiple".getBytes()));

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ONE);
    filter2.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp));
    filter2.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp2));

    FilterList filter3 = new FilterList(Operator.MUST_PASS_ALL);
    filter3.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp));
    filter3.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "multiple".getBytes(), vp2));

    FilterList filter4 = new FilterList(Operator.MUST_PASS_ALL);
    filter3.addFilter(new SingleColumnValueFilter(hcd.getName(), "cq1".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "1234".getBytes()));
    filter3.addFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.GREATER_OR_EQUAL, "multiple".getBytes(), vp2));
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    masterFilter.addFilter(filter3);
    masterFilter.addFilter(filter4);

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
    assertTrue("Overall result should have only 1 rows", testRes.size() == 1);
  }

  @Test(timeout = 180000)
  public void testSingleColumnValuePartitionFilterBySettingAsAttributeToScan() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSingleColumnValuePartitionFilterBySettingAsAttributeToScan";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    TableIndices indices = new TableIndices();
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    ValuePartition vp = new SeparatorPartition("_", 3);
    IndexSpecification iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "cq", vp, ValueType.String, 200);
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    HTable table = new HTable(conf, userTableName);
    byte[] value1 = "2ndFloor_solitaire_huawei_bangalore_karnataka".getBytes();
    Put p = new Put("row".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), value1);
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(),
      "7thFloor_solitaire_huawei_bangalore_karnataka".getBytes());
    table.put(p);

    p = new Put("row3".getBytes());
    p.add("cf1".getBytes(), "cq".getBytes(), "rrr_sss_hhh_bangalore_karnataka".getBytes());
    table.put(p);

    Scan scan = new Scan();
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression("idx1");
    byte[] value = "huawei".getBytes();
    Column column = new Column("cf1".getBytes(), "cq".getBytes(), vp);
    EqualsExpression equalsExpression = new EqualsExpression(column, value);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    scan.setAttribute(Constants.INDEX_EXPRESSION, IndexUtils.toBytes(singleIndexExpression));
    scan.setFilter(new SingleColumnValuePartitionFilter(hcd.getName(), "cq".getBytes(),
        CompareOp.EQUAL, "huawei".getBytes(), vp));
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

}
