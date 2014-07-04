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
package org.apache.hadoop.hbase.index.coprocessor.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Tests invocation of the {@link IndexMasterObserver} coprocessor hooks at all appropriate times
 * during normal HMaster operations.
 */

@Category(LargeTests.class)
public class TestIndexMasterObserver {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static HBaseAdmin admin;
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,true);
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set("index.data.block.encoding.algo", "PREFIX");
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    UTIL.startMiniCluster(1);
    admin = new IndexAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // Check this test case why its failing.
  @Test(timeout = 180000)
  public void testCreateTableWithIndexTableSuffix() throws Exception {
    HTableDescriptor htd =
        TestUtils.createIndexedHTableDescriptor("testCreateTableWithIndexTableSuffix"
            + Constants.INDEX_TABLE_SUFFIX, "cf", "index_name", "cf", "cq");
    try {
      admin.createTable(htd);
      fail("User table should not ends with " + Constants.INDEX_TABLE_SUFFIX);
    } catch (IOException e) {

    }
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenIndexTableAlreadyExist() throws Exception {
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testCreateIndexTableWhenIndexTableAlreadyExist", "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);

    admin.disableTable("testCreateIndexTableWhenIndexTableAlreadyExist");
    admin.deleteTable("testCreateIndexTableWhenIndexTableAlreadyExist");

    admin.createTable(iHtd);

    assertTrue("Table is not created.",
      admin.isTableAvailable("testCreateIndexTableWhenIndexTableAlreadyExist"));

    String indexTableName =
        "testCreateIndexTableWhenIndexTableAlreadyExist" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index table is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenBothIndexAndUserTableExist() throws Exception {
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testCreateIndexTableWhenBothIndexAndUserTableExist", "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);

    try {
      admin.createTable(iHtd);
      fail("Should throw TableExistsException " + "if both user table and index table exist.");
    } catch (TableExistsException t) {

    }
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWithOutIndexDetails() throws Exception {
    HTableDescriptor iHtd =
        new HTableDescriptor(TableName.valueOf("testCreateIndexTableWithOutIndexDetails"));
    TableIndices indices = new TableIndices();
    iHtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(iHtd);
    assertTrue("Table is not created.",
      admin.isTableAvailable("testCreateIndexTableWithOutIndexDetails"));

    String indexTableName =
        "testCreateIndexTableWithOutIndexDetails" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedIndexTableDisabled() throws Exception {
    String tableName = "testCreateIndexTableWhenExistedIndexTableDisabled";
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);

    iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);
    assertTrue("Table is not created.",
      admin.isTableAvailable(tableName));

    String indexTableName = IndexUtils.getIndexTableName(tableName);
    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedTableDisableFailed() throws Exception {
    MiniHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    HMaster master = cluster.getMaster();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testCreateIndexTableWhenExistedTableDisableFailed"));
    admin.createTable(htd);

    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testCreateIndexTableWhenExistedTableDisableFailed", "cf",
          "index_name", "cf", "cq");
    IndexMasterObserver ms = Mockito.mock(IndexMasterObserver.class);
    Mockito
        .doThrow(new RuntimeException())
        .when(ms)
        .preCreateTable((ObserverContext<MasterCoprocessorEnvironment>) Mockito.anyObject(),
          (HTableDescriptor) Mockito.anyObject(), (HRegionInfo[]) Mockito.anyObject());

    try {
      char c = 'A';
      byte[][] split = new byte[20][];
      for (int i = 0; i < 20; i++) {
        byte[] b = { (byte) c };
        split[i] = b;
        c++;
      }
      admin.createTable(iHtd, split);
      assertFalse(master.getAssignmentManager().getZKTable()
          .isDisabledTable(TableName.valueOf("testCreateIndexTableWhenExistedTableDisableFailed")));
      // fail("Should throw RegionException.");
    } catch (IOException r) {
    } 
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedTableDeleteFailed() throws Exception {
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testCreateIndexTableWhenExistedTableDeleteFailed", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    admin.disableTable("testCreateIndexTableWhenExistedTableDeleteFailed");
    admin.deleteTable("testCreateIndexTableWhenExistedTableDeleteFailed");
    IndexMasterObserver ms = Mockito.mock(IndexMasterObserver.class);
    Mockito
        .doThrow(new RuntimeException())
        .when(ms)
        .preCreateTable((ObserverContext<MasterCoprocessorEnvironment>) Mockito.anyObject(),
          (HTableDescriptor) Mockito.anyObject(), (HRegionInfo[]) Mockito.anyObject());
    try {
      admin.createTable(iHtd);
    } catch (IOException e) {

    }
  }

  @Test(timeout = 180000)
  public void testIndexTableCreationAfterMasterRestart() throws Exception {
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testIndexTableCreationAfterMasterRestart", "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable("testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX);
    admin.deleteTable("testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    // start up a new master
    cluster.startMaster();
    assertTrue(cluster.waitForActiveAndReadyMaster());
    String indexTableName =
        "testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testIndexTableCreationAlongWithNormalTablesAfterMasterRestart() throws Exception {
    TableName tableName =
        TableName.valueOf("testIndexTableCreationAlongWithNormalTablesAfterMasterRestart");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    admin.createTable(htd);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    HMaster master = cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();

    boolean tableExist =
        MetaReader.tableExists(master.getCatalogTracker(),
          TableName.valueOf(IndexUtils.getIndexTableName(tableName)));
    assertFalse("Index table should be not created after master start up.", tableExist);
  }

  // Check this test case.
  @Test(timeout = 180000)
  public void testPreCreateShouldNotBeSuccessfulIfIndicesAreNotSame() throws IOException,
      KeeperException, InterruptedException {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex1";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    boolean returnVal = false;
    try {
      admin.createTable(ihtd);
      fail("Exception should be thrown");
    } catch (IOException e) {

      returnVal = true;
    }
    Assert.assertTrue(returnVal);
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldNotBeSuccessfulIfIndicesAreNotSameAtLength() throws IOException,
      KeeperException, InterruptedException {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex2";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 4);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q3", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd, "q2", ValueType.String, 10);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    boolean returnVal = false;
    try {
      admin.createTable(ihtd);
      fail("Exception should be thrown");
    } catch (IOException e) {
      returnVal = true;
    }
    Assert.assertTrue(returnVal);
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldNotBeSuccessfulIfIndicesAreNotSameAtType() throws IOException,
      KeeperException, InterruptedException {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex3";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    iSpec2.addIndexColumn(hcd, "q3", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    boolean returnVal = false;
    try {
      admin.createTable(ihtd);
      fail("Exception should be thrown");
    } catch (IOException e) {
      returnVal = true;
    }
    Assert.assertTrue(returnVal);
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldNotBeSuccessfulIfIndicesAreNotSameAtBothTypeAndLength()
      throws IOException, KeeperException, InterruptedException {
    String userTableName = "testNotConsisIndex4";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    iSpec2.addIndexColumn(hcd, "q2", ValueType.String, 7);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    boolean returnVal = false;
    try {
      admin.createTable(ihtd);
      fail("IOException should be thrown");
    } catch (IOException e) {
      returnVal = true;
    }
    Assert.assertTrue(returnVal);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldBeSuccessfulIfIndicesAreSame() throws IOException,
      KeeperException, InterruptedException {
    String userTableName = "testConsistIndex";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    try {
      admin.createTable(ihtd);
    } catch (IOException e) {
      fail("Exception should not be thrown");
    }
  }

  @Test(timeout = 180000)
  public void testIndexTableShouldBeDisabledIfUserTableDisabled() throws Exception {
    String tableName = "testIndexTableDisabledIfUserTableDisabled";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(tableName);
    assertTrue("User table should be disabled.", admin.isTableDisabled(tableName));
    assertTrue("Index table should be disabled.", admin.isTableDisabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testIndexTableShouldBeEnabledIfUserTableEnabled() throws Exception {
    String tableName = "testIndexTableEnabledIfUserTableEnabled";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    assertTrue("User table should be enabled.", admin.isTableEnabled(tableName));
    assertTrue("Index table should be enabled.", admin.isTableEnabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testDisabledIndexTableShouldBeEnabledIfUserTableEnabledAndMasterRestarted()
      throws Exception {
    String tableName = "testDisabledIndexTableEnabledIfUserTableEnabledAndMasterRestarted";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(indexTableName);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.startMaster();
    cluster.waitOnMaster(0);
    cluster.waitForActiveAndReadyMaster();
    Thread.sleep(1000);
    assertTrue("User table should be enabled.", admin.isTableEnabled(tableName));
    assertTrue("Index table should be enabled.", admin.isTableEnabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testEnabledIndexTableShouldBeDisabledIfUserTableDisabledAndMasterRestarted()
      throws Exception {
    String tableName = "testEnabledIndexTableDisabledIfUserTableDisabledAndMasterRestarted";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(tableName);
    admin.enableTable(indexTableName);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.startMaster();
    cluster.waitOnMaster(0);
    cluster.waitForActiveAndReadyMaster();
    Thread.sleep(1000);
    assertTrue("User table should be disabled.", admin.isTableDisabled(tableName));
    assertTrue("Index table should be disabled.", admin.isTableDisabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testDisabledIndexTableShouldBeEnabledIfUserTableInEnablingAndMasterRestarted()
      throws Exception {
    String tableName = "testDisabledIndexTableEnabledIfUserTableInEnablingAndMasterRestarted";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(indexTableName);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getAssignmentManager().getZKTable().setEnablingTable(TableName.valueOf(tableName));
    cluster.abortMaster(0);
    cluster.startMaster();
    cluster.waitOnMaster(0);
    cluster.waitForActiveAndReadyMaster();
    Thread.sleep(1000);
    assertTrue("User table should be enabled.", admin.isTableEnabled(tableName));
    assertTrue("Index table should be enabled.", admin.isTableEnabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testEnabledIndexTableShouldBeDisabledIfUserTableInDisablingAndMasterRestarted()
      throws Exception {
    String tableName = "testEnabledIndexTableDisabledIfUserTableInDisablingAndMasterRestarted";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getAssignmentManager().getZKTable().setDisablingTable(TableName.valueOf(tableName));
    cluster.abortMaster(0);
    cluster.startMaster();
    cluster.waitOnMaster(0);
    cluster.waitForActiveAndReadyMaster();
    Thread.sleep(1000);
    assertTrue("User table should be disabled.", admin.isTableDisabled(tableName));
    assertTrue("Index table should be disabled.", admin.isTableDisabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testShouldModifyTableWithIndexDetails() throws Exception {
    String tableName = "testShouldModifyTableWithIndexDetails";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    admin.createTable(htd);
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    admin.disableTable(tableName);
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "f1", "idx1", "f1", "q1");
    admin.modifyTable(Bytes.toBytes(tableName), ihtd);
    List<HRegionInfo> regionsOfTable =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
            .getRegionsOfTable(indexTableName);
    while (regionsOfTable.size() != 1) {
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTableName);
    }
    admin.enableTable(tableName);
    assertTrue(admin.isTableEnabled(Bytes.toBytes(IndexUtils.getIndexTableName(tableName))));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableFromExistingTable() throws Exception {
    String tableName = "testCreateIndexTableFromExistingTable";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    byte[][] split = new byte[][] { Bytes.toBytes("A"), Bytes.toBytes("B") };
    admin.createTable(htd, split);
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    HTable table = new HTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("f1"), Bytes.toBytes("q1"), Bytes.toBytes("2"));
    p.add(Bytes.toBytes("f2"), Bytes.toBytes("q2"), Bytes.toBytes("3"));
    table.put(p);
    table.flushCommits();
    admin.flush(tableName);
    UTIL.getConfiguration().set("table.columns.index",
      "IDX1=>f1:[q1->Int&10],[q2],[q3];f2:[q1->String&15],[q2->Int&15]#IDX2=>f1:[q5]");
    IndexUtils.createIndexTable(tableName, UTIL.getConfiguration(), null);
    List<HRegionInfo> regionsOfTable =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTableName);
    while (regionsOfTable.size() != 3) {
      Thread.sleep(500);
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTableName);
    }
    HTableDescriptor indexedTableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
    byte[] value = indexedTableDesc.getValue(Constants.INDEX_SPEC_KEY);
    TableIndices indices = new TableIndices();
    indices.readFields(value);
    assertEquals(indices.getIndices().size(), 2);
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result[] next = scanner.next(10);
    List<Cell> cf1 = next[0].getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    List<Cell> cf2 = next[0].getColumnCells(Bytes.toBytes("f2"), Bytes.toBytes("q2"));
    assertTrue(cf1.size() > 0 && cf2.size() > 0);
  }

  @Test(timeout = 180000)
  public void testShouldRetainTheExistingCFsInHTD() throws Exception {
    String tableName = "testShouldRetainTheExistingCFsInHTD";
    HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    admin.createTable(htd);
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    HTable table = new HTable(admin.getConfiguration(), tableName);
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("f1"), Bytes.toBytes("q1"), Bytes.toBytes("2"));
    p.add(Bytes.toBytes("f2"), Bytes.toBytes("q2"), Bytes.toBytes("3"));
    table.put(p);
    table.flushCommits();
    admin.flush(tableName);
    UTIL.getConfiguration().set("table.columns.index", "IDX1=>f1:[q1->Int&10],[q2],[q3];");
    IndexUtils.createIndexTable(tableName, UTIL.getConfiguration(), null);
    List<HRegionInfo> regionsOfTable =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTableName);
    while (regionsOfTable.size() != 1) {
      Thread.sleep(500);
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTableName);
    }
    HTableDescriptor indexedTableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
    byte[] value = indexedTableDesc.getValue(Constants.INDEX_SPEC_KEY);
    TableIndices indices = new TableIndices();
    indices.readFields(value);
    assertEquals(indices.getIndices().size(), 1);
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result[] next = scanner.next(10);
    List<Cell> cf1 = next[0].getColumnCells(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    List<Cell> cf2 = next[0].getColumnCells(Bytes.toBytes("f2"), Bytes.toBytes("q2"));
    assertTrue(cf1.size() > 0 && cf2.size() > 0);
  }

  @Test(timeout = 180000)
  public void testBlockEncoding() throws Exception {
    Configuration conf = admin.getConfiguration();
    String userTableName = "testBlockEncoding";
    HTableDescriptor htd = TestUtils.createIndexedHTableDescriptor(userTableName, "col1", "Index1", "col1", "ql");
    admin.createTable(htd);
    HTableDescriptor tableDescriptor =
        admin.getTableDescriptor(Bytes.toBytes(IndexUtils.getIndexTableName(userTableName)));
    assertEquals(DataBlockEncoding.PREFIX,
      tableDescriptor.getColumnFamilies()[0].getDataBlockEncoding());
  }
}
