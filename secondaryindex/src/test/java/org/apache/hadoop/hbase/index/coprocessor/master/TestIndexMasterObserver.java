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
package org.apache.hadoop.hbase.index.coprocessor.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableExistsException;
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
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
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
 * Tests invocation of the {@link MasterObserverImpl} coprocessor hooks at all appropriate times
 * during normal HMaster operations.
 */

@Category(LargeTests.class)
public class TestIndexMasterObserver {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set("index.data.block.encoding.algo", "PREFIX");
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testCreateTableWithIndexTableSuffix() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    IndexedHTableDescriptor htd =
        createIndexedHTableDescriptor("testCreateTableWithIndexTableSuffix"
            + Constants.INDEX_TABLE_SUFFIX, "cf", "index_name", "cf", "cq");
    try {
      admin.createTable(htd);
      fail("User table should not ends with " + Constants.INDEX_TABLE_SUFFIX);
    } catch (IOException e) {

    }
  }

  private IndexedHTableDescriptor createIndexedHTableDescriptor(String tableName,
      String columnFamily, String indexName, String indexColumnFamily, String indexColumnQualifier) {
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor(tableName);
    IndexSpecification iSpec = new IndexSpecification(indexName);
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    iSpec.addIndexColumn(hcd, indexColumnQualifier, ValueType.String, 10);
    htd.addFamily(hcd);
    htd.addIndex(iSpec);
    return htd;
  }

  /*
   * @Test public void testCreateIndexTableWhenUserTableAlreadyExist() throws Exception { HBaseAdmin
   * admin = UTIL.getHBaseAdmin(); MiniHBaseCluster cluster = UTIL.getHBaseCluster(); HMaster master
   * = cluster.getMaster(); HTableDescriptor htd = new HTableDescriptor(
   * "testCreateIndexTableWhenUserTableAlreadyExist"); admin.createTable(htd);
   * IndexedHTableDescriptor iHtd = createIndexedHTableDescriptor(
   * "testCreateIndexTableWhenUserTableAlreadyExist", "cf", "index_name", "cf", "cq"); char c = 'A';
   * byte[][] split = new byte[1][]; for (int i = 0; i < 1; i++) { byte[] b = { (byte) c }; split[i]
   * = b; c++; } admin.createTable(iHtd,split); assertTrue("Table is not created.",admin
   * .isTableAvailable("testCreateIndexTableWhenUserTableAlreadyExist")); String tableName =
   * "testCreateIndexTableWhenUserTableAlreadyExist" + Constants.INDEX_TABLE_SUFFIX;
   * waitUntilIndexTableCreated(master, tableName); assertTrue( "Index table is not created.",
   * admin.isTableAvailable(tableName)); }
   */

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenIndexTableAlreadyExist() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenIndexTableAlreadyExist", "cf",
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
    HBaseAdmin admin = UTIL.getHBaseAdmin();

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenBothIndexAndUserTableExist", "cf",
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
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();

    IndexedHTableDescriptor iHtd =
        new IndexedHTableDescriptor("testCreateIndexTableWithOutIndexDetails");
    admin.createTable(iHtd);
    assertTrue("Table is not created.",
      admin.isTableAvailable("testCreateIndexTableWithOutIndexDetails"));

    String indexTableName =
        "testCreateIndexTableWithOutIndexDetails" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedIndexTableDisabled() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenExistedIndexTableDisabled", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    admin.disableTable("testCreateIndexTableWhenExistedIndexTableDisabled");
    admin.deleteTable("testCreateIndexTableWhenExistedIndexTableDisabled");

    iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenExistedIndexTableDisabled", "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);
    assertTrue("Table is not created.",
      admin.isTableAvailable("testCreateIndexTableWhenExistedIndexTableDisabled"));

    String indexTableName =
        "testCreateIndexTableWhenExistedIndexTableDisabled" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));

  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedTableDisableFailed() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    HMaster master = cluster.getMaster();
    HTableDescriptor htd =
        new HTableDescriptor("testCreateIndexTableWhenExistedTableDisableFailed");
    admin.createTable(htd);

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenExistedTableDisableFailed", "cf",
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
          .isDisabledTable("testCreateIndexTableWhenExistedTableDisableFailed"));
      // fail("Should throw RegionException.");
    } catch (IOException r) {

    } finally {
    }
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableWhenExistedTableDeleteFailed() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testCreateIndexTableWhenExistedTableDeleteFailed", "cf",
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testIndexTableCreationAfterMasterRestart", "cf",
          "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable("testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX);
    admin.deleteTable("testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    HMaster master = cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();
    String indexTableName =
        "testIndexTableCreationAfterMasterRestart" + Constants.INDEX_TABLE_SUFFIX;

    assertTrue("Index tables is not created.", admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testIndexTableCreationAlongWithNormalTablesAfterMasterRestart() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());

    HTableDescriptor htd =
        new HTableDescriptor("testIndexTableCreationAlongWithNormalTablesAfterMasterRestart");
    admin.createTable(htd);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    HMaster master = cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();

    boolean tableExist =
        MetaReader.tableExists(master.getCatalogTracker(),
          "testIndexTableCreationAlongWithNormalTablesAfterMasterRestart"
              + Constants.INDEX_TABLE_SUFFIX);
    assertFalse("Index table should be not created after master start up.", tableExist);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldNotBeSuccessfulIfIndicesAreNotSame() throws IOException,
      KeeperException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex1";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    ihtd.addIndex(iSpec2);

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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex2";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 4);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q3", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addIndex(iSpec2);

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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex3";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    iSpec2.addIndexColumn(hcd, "q3", ValueType.String, 10);
    ihtd.addIndex(iSpec2);
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testNotConsisIndex4";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.Int, 10);
    iSpec2.addIndexColumn(hcd, "q2", ValueType.String, 7);
    ihtd.addIndex(iSpec2);

    boolean returnVal = false;
    try {
      admin.createTable(ihtd);
      fail("IOException should be thrown");
    } catch (IOException e) {
      returnVal = true;
    }
    Assert.assertTrue(returnVal);
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test(timeout = 180000)
  public void testPreCreateShouldBeSuccessfulIfIndicesAreSame() throws IOException,
      KeeperException, InterruptedException {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testConsisIndex";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec1);
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addIndex(iSpec2);

    try {
      admin.createTable(ihtd);
    } catch (IOException e) {
      fail("Exception should not be thrown");
    }
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test(timeout = 180000)
  public void testIndexTableShouldBeDisabledIfUserTableDisabled() throws Exception {
    String tableName = "testIndexTableDisabledIfUserTableDisabled";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(tableName);
    assertTrue("User table should be disabled.", admin.isTableDisabled(tableName));
    assertTrue("Index table should be disabled.", admin.isTableDisabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testIndexTableShouldBeEnabledIfUserTableEnabled() throws Exception {
    String tableName = "testIndexTableEnabledIfUserTableEnabled";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(indexTableName);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getAssignmentManager().getZKTable().setEnablingTable(tableName);
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getAssignmentManager().getZKTable().setDisablingTable(tableName);
    cluster.abortMaster(0);
    cluster.startMaster();
    cluster.waitOnMaster(0);
    cluster.waitForActiveAndReadyMaster();
    Thread.sleep(1000);
    assertTrue("User table should be disabled.", admin.isTableDisabled(tableName));
    assertTrue("Index table should be disabled.", admin.isTableDisabled(indexTableName));
  }

  @Test(timeout = 180000)
  public void testIndexTableShouldBeDeletedIfUserTableDeleted() throws Exception {
    String tableName = "testIndexTableDeletedIfUserTableDeleted";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    admin.createTable(iHtd);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    assertFalse("User table should not be available after deletion.",
      admin.isTableAvailable(tableName));
    assertFalse("Index table should not be available after deletion.",
      admin.isTableAvailable(indexTableName));
  }

  @Test(timeout = 180000)
  public void testShouldModifyTableWithIndexDetails() throws Exception {
    String tableName = "testShouldModifyTableWithIndexDetails";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    admin.createTable(htd);

    admin.disableTable(tableName);
    IndexedHTableDescriptor ihtd =
        createIndexedHTableDescriptor(tableName, "f1", "idx1", "f1", "q1");
    admin.modifyTable(Bytes.toBytes(tableName), ihtd);
    List<HRegionInfo> regionsOfTable =
        UTIL.getHBaseCluster().getMaster().getAssignmentManager()
            .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    while (regionsOfTable.size() != 1) {
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    }
    admin.enableTable(tableName);
    assertTrue(admin.isTableEnabled(Bytes.toBytes(tableName + "_idx")));
  }

  @Test(timeout = 180000)
  public void testCreateIndexTableFromExistingTable() throws Exception {
    String tableName = "testCreateIndexTableFromExistingTable";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    byte[][] split = new byte[][] { Bytes.toBytes("A"), Bytes.toBytes("B") };
    admin.createTable(htd, split);
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
            .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    while (regionsOfTable.size() != 3) {
      Thread.sleep(500);
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    }
    MasterFileSystem masterFileSystem = UTIL.getHBaseCluster().getMaster().getMasterFileSystem();
    Path path = FSUtils.getTablePath(masterFileSystem.getRootDir(), htd.getName());
    FileSystem fs = masterFileSystem.getFileSystem();
    FileStatus status = getTableInfoPath(fs, path);
    if (null == status) {
      fail("Status should not be null");
    }
    FSDataInputStream fsDataInputStream = fs.open(status.getPath());
    HTableDescriptor iHtd = new IndexedHTableDescriptor();
    iHtd.readFields(fsDataInputStream);
    assertEquals(((IndexedHTableDescriptor) iHtd).getIndices().size(), 2);
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result[] next = scanner.next(10);
    List<KeyValue> cf1 = next[0].getColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    List<KeyValue> cf2 = next[0].getColumn(Bytes.toBytes("f2"), Bytes.toBytes("q2"));
    assertTrue(cf1.size() > 0 && cf2.size() > 0);
  }

  @Test(timeout = 180000)
  public void testShouldRetainTheExistingCFsInHTD() throws Exception {
    String tableName = "testShouldRetainTheExistingCFsInHTD";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f1")));
    htd.addFamily(new HColumnDescriptor(Bytes.toBytes("f2")));
    admin.createTable(htd);
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
            .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    while (regionsOfTable.size() != 1) {
      Thread.sleep(500);
      regionsOfTable =
          UTIL.getHBaseCluster().getMaster().getAssignmentManager()
              .getRegionsOfTable(Bytes.toBytes(tableName + "_idx"));
    }
    MasterFileSystem masterFileSystem = UTIL.getHBaseCluster().getMaster().getMasterFileSystem();
    Path path = FSUtils.getTablePath(masterFileSystem.getRootDir(), htd.getName());
    FileSystem fs = masterFileSystem.getFileSystem();
    FileStatus status = getTableInfoPath(fs, path);
    if (null == status) {
      fail("Status should not be null");
    }
    FSDataInputStream fsDataInputStream = fs.open(status.getPath());
    HTableDescriptor iHtd = new IndexedHTableDescriptor();
    iHtd.readFields(fsDataInputStream);
    assertEquals(((IndexedHTableDescriptor) iHtd).getIndices().size(), 1);
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result[] next = scanner.next(10);
    List<KeyValue> cf1 = next[0].getColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    List<KeyValue> cf2 = next[0].getColumn(Bytes.toBytes("f2"), Bytes.toBytes("q2"));
    assertTrue(cf1.size() > 0 && cf2.size() > 0);
  }

  @Test(timeout = 180000)
  public void testBlockEncoding() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    Configuration conf = admin.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testBlockEncoding";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col1");

    IndexSpecification iSpec = new IndexSpecification("Index1");

    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);

    ihtd.addIndex(iSpec);
    admin.createTable(ihtd);
    ZKAssign.blockUntilNoRIT(zkw);
    HTable table = new HTable(conf, userTableName + "_idx");
    HTableDescriptor tableDescriptor =
        admin.getTableDescriptor(Bytes.toBytes(userTableName + "_idx"));
    assertEquals(DataBlockEncoding.PREFIX,
      tableDescriptor.getColumnFamilies()[0].getDataBlockEncoding());
  }

  private static FileStatus getTableInfoPath(final FileSystem fs, final Path tabledir)
      throws IOException {
    FileStatus[] status = FSUtils.listStatus(fs, tabledir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        // Accept any file that starts with TABLEINFO_NAME
        return p.getName().startsWith(FSTableDescriptors.TABLEINFO_NAME);
      }
    });
    if (status == null || status.length < 1) return null;
    Arrays.sort(status, new FileStatusFileNameComparator());
    if (status.length > 1) {
      // Clean away old versions of .tableinfo
      for (int i = 1; i < status.length; i++) {
        Path p = status[i].getPath();
        // Clean up old versions
        if (!fs.delete(p, false)) {
        } else {
        }
      }
    }
    return status[0];
  }

  /**
   * Compare {@link FileStatus} instances by {@link Path#getName()}. Returns in reverse order.
   */
  private static class FileStatusFileNameComparator implements Comparator<FileStatus> {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return -left.compareTo(right);
    }
  }
}
