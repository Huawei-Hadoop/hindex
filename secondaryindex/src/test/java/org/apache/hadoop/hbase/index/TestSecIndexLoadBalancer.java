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
package org.apache.hadoop.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSecIndexLoadBalancer {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 4;
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentDuringIndexTableCreation() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    IndexedHTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRoundRobinAssignmentDuringIndexTableCreation",
          "cf", "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    String tableName = "testRoundRobinAssignmentDuringIndexTableCreation";
    String indexTableName =
        "testRoundRobinAssignmentDuringIndexTableCreation" + Constants.INDEX_TABLE_SUFFIX;
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentDuringIndexTableEnable() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);

    IndexedHTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRoundRobinAssignmentDuringIndexTableEnable",
          "cf", "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    String tableName = "testRoundRobinAssignmentDuringIndexTableEnable";
    String indexTableName =
        "testRoundRobinAssignmentDuringIndexTableEnable" + Constants.INDEX_TABLE_SUFFIX;
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testColocationAfterSplit() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testCompactionOnIndexTableShouldNotRetrieveTTLExpiredData";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);

    HColumnDescriptor hcd = new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE);

    HColumnDescriptor hcd1 = new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd);
    ZKAssign.blockUntilNoRIT(zkw);

    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column

    Put p = new Put("row11".getBytes());
    p.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p);

    Put p1 = new Put("row21".getBytes());
    p1.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p1.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p1);

    Put p2 = new Put("row31".getBytes());
    p2.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p2.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p2);

    Put p3 = new Put("row41".getBytes());
    p3.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p3.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p3);

    Put p4 = new Put("row51".getBytes());
    p4.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p4.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p4);

    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");

    admin.split(userTableName, "row31");
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    List<HRegionInfo> regionsOfUserTable =
        master.getAssignmentManager().getRegionsOfTable(Bytes.toBytes(userTableName));

    while (regionsOfUserTable.size() != 2) {
      regionsOfUserTable =
          master.getAssignmentManager().getRegionsOfTable(Bytes.toBytes(userTableName));
    }

    List<HRegionInfo> regionsOfIndexTable =
        master.getAssignmentManager().getRegionsOfTable(Bytes.toBytes(userTableName + "_idx"));

    while (regionsOfIndexTable.size() != 2) {
      regionsOfIndexTable =
          master.getAssignmentManager().getRegionsOfTable(Bytes.toBytes(userTableName + "_idx"));
    }

    for (HRegionInfo hRegionInfo : regionsOfUserTable) {
      for (HRegionInfo indexRegionInfo : regionsOfIndexTable) {
        if (Bytes.equals(hRegionInfo.getStartKey(), indexRegionInfo.getStartKey())) {
          assertEquals("The regions should be colocated", master.getAssignmentManager()
              .getRegionServerOfRegion(hRegionInfo), master.getAssignmentManager()
              .getRegionServerOfRegion(indexRegionInfo));
        }
      }
    }

  }

  @Test(timeout = 180000)
  public void testRandomAssignmentDuringIndexTableEnable() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
    IndexedHTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRandomAssignmentDuringIndexTableEnable", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[3][];
    for (int i = 0; i < 3; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);

    String tableName = "testRandomAssignmentDuringIndexTableEnable";
    String indexTableName =
        "testRandomAssignmentDuringIndexTableEnable" + Constants.INDEX_TABLE_SUFFIX;
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    boolean isRegionColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testBalanceClusterFromAdminBalancer() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);
    master.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", false);

    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testBalanceClusterFromAdminBalancer", "cf", "index_name",
          "cf", "cq");
    HTableDescriptor htd = new HTableDescriptor("testBalanceClusterFromAdminBalancer1");
    htd.addFamily(new HColumnDescriptor("fam1"));

    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    c = 'A';
    byte[][] split1 = new byte[12][];
    for (int i = 0; i < 12; i++) {
      byte[] b = { (byte) c };
      split1[i] = b;
      c++;
    }
    admin.balanceSwitch(false);
    admin.createTable(htd, split1);
    String tableName = "testBalanceClusterFromAdminBalancer";
    String indexTableName = "testBalanceClusterFromAdminBalancer" + Constants.INDEX_TABLE_SUFFIX;
    waitUntilIndexTableCreated(master, indexTableName);
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    admin.balanceSwitch(true);
    admin.balancer();
    boolean isRegionsColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionsColocated);
  }

  @Test(timeout = 180000)
  public void testByTableBalanceClusterFromAdminBalancer() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testByTableBalanceClusterFromAdminBalancer", "cf",
          "index_name", "cf", "cq");
    HTableDescriptor htd = new HTableDescriptor("testByTableBalanceClusterFromAdminBalancer1");
    htd.addFamily(new HColumnDescriptor("fam1"));

    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    c = 'A';
    byte[][] split1 = new byte[12][];
    for (int i = 0; i < 12; i++) {
      byte[] b = { (byte) c };
      split1[i] = b;
      c++;
    }
    admin.balanceSwitch(false);
    admin.createTable(htd, split1);
    String tableName = "testByTableBalanceClusterFromAdminBalancer";
    String indexTableName =
        "testByTableBalanceClusterFromAdminBalancer" + Constants.INDEX_TABLE_SUFFIX;
    waitUntilIndexTableCreated(master, indexTableName);
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    admin.balanceSwitch(true);
    admin.balancer();
    int totalNumRegions = 21;
    Thread.sleep(10000);
    List<Pair<byte[], ServerName>> iTableStartKeysAndLocations = null;
    do {
      iTableStartKeysAndLocations = getStartKeysAndLocations(master, indexTableName);
      System.out.println("wait until all regions comeup " + iTableStartKeysAndLocations.size());
      Thread.sleep(1000);
    } while (totalNumRegions > iTableStartKeysAndLocations.size());
    ZKAssign.blockUntilNoRIT(zkw);
    boolean isRegionColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
  }

  @Test(timeout = 180000)
  public void testRandomAssignmentAfterRegionServerDown() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testRandomAssignmentAfterRegionServerDown", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);

    String tableName = "testRandomAssignmentAfterRegionServerDown";
    String indexTableName =
        "testRandomAssignmentAfterRegionServerDown" + Constants.INDEX_TABLE_SUFFIX;
    HRegionServer regionServer = cluster.getRegionServer(1);
    regionServer.abort("Aborting to test random assignment after region server down");
    while (master.getServerManager().areDeadServersInProgress()) {
      Thread.sleep(1000);
    }
    int totalNumRegions = 21;
    List<Pair<byte[], ServerName>> iTableStartKeysAndLocations = null;
    do {
      iTableStartKeysAndLocations = getStartKeysAndLocations(master, indexTableName);
      System.out.println("wait until all regions comeup " + iTableStartKeysAndLocations.size());
      Thread.sleep(1000);
    } while (totalNumRegions > iTableStartKeysAndLocations.size());
    ZKAssign.blockUntilNoRIT(zkw);
    boolean isRegionColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testPostAssign() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    ZKAssign.deleteAllNodes(zkw);
    IndexedHTableDescriptor iHtd = new IndexedHTableDescriptor("testPostAssign");
    char c = 'A';
    byte[][] split = new byte[4][];
    for (int i = 0; i < 4; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    List<HRegionInfo> tableRegions =
        MetaReader.getTableRegions(master.getCatalogTracker(),
          Bytes.toBytes(iHtd.getNameAsString()));
    for (HRegionInfo hRegionInfo : tableRegions) {
      admin.assign(hRegionInfo.getRegionName());
    }
    ZKAssign.blockUntilNoRIT(zkw);
    boolean isRegionColocated =
        checkForColocation(master, "testPostAssign", "testPostAssign"
            + Constants.INDEX_TABLE_SUFFIX);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testRegionMove() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);

    IndexedHTableDescriptor iHtd = new IndexedHTableDescriptor("testRegionMove");
    char c = 'A';
    byte[][] split = new byte[4][];
    for (int i = 0; i < 4; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    List<HRegionInfo> tableRegions =
        MetaReader.getTableRegions(master.getCatalogTracker(),
          Bytes.toBytes(iHtd.getNameAsString()));
    int numRegions = cluster.getRegionServerThreads().size();
    cluster.getRegionServer(1).getServerName();
    Random random = new Random();
    for (HRegionInfo hRegionInfo : tableRegions) {
      int regionNumber = random.nextInt(numRegions);
      ServerName serverName = cluster.getRegionServer(regionNumber).getServerName();
      admin.move(hRegionInfo.getEncodedNameAsBytes(), Bytes.toBytes(serverName.getServerName()));
    }
    ZKAssign.blockUntilNoRIT(zkw);
    boolean isRegionColocated =
        checkForColocation(master, "testRegionMove", "testRegionMove"
            + Constants.INDEX_TABLE_SUFFIX);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testRetainAssignmentDuringMasterStartUp() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", true);
    IndexedHTableDescriptor iHtd =
        createIndexedHTableDescriptor("testRetainAssignmentDuringMasterStartUp", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    String tableName = "testRetainAssignmentDuringMasterStartUp";
    String indexTableName =
        "testRetainAssignmentDuringMasterStartUp" + Constants.INDEX_TABLE_SUFFIX;
    waitUntilIndexTableCreated(master, indexTableName);
    UTIL.shutdownMiniHBaseCluster();
    UTIL.startMiniHBaseCluster(1, 4);
    cluster = UTIL.getHBaseCluster();
    master = cluster.getMaster();

    boolean isRegionColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentDuringMasterStartUp() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);

    IndexedHTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRoundRobinAssignmentDuringMasterStartUp",
          "cf", "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    String tableName = "testRoundRobinAssignmentDuringMasterStartUp";
    String indexTableName =
        "testRoundRobinAssignmentDuringMasterStartUp" + Constants.INDEX_TABLE_SUFFIX;
    waitUntilIndexTableCreated(master, indexTableName);
    UTIL.shutdownMiniHBaseCluster();
    UTIL.startMiniHBaseCluster(1, 4);
    cluster = UTIL.getHBaseCluster();
    master = cluster.getMaster();
    boolean isRegionColocated = checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

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

  private void waitUntilIndexTableCreated(HMaster master, String tableName) throws IOException,
      InterruptedException {
    boolean isEnabled = false;
    boolean isExist = false;
    do {
      isExist = MetaReader.tableExists(master.getCatalogTracker(), tableName);
      isEnabled = master.getAssignmentManager().getZKTable().isEnabledTable(tableName);
      Thread.sleep(1000);
    } while ((false == isExist) && (false == isEnabled));
  }

  private List<Pair<byte[], ServerName>> getStartKeysAndLocations(HMaster master, String tableName)
      throws IOException, InterruptedException {

    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations =
        MetaReader.getTableRegionsAndLocations(master.getCatalogTracker(), tableName);
    List<Pair<byte[], ServerName>> startKeyAndLocationPairs =
        new ArrayList<Pair<byte[], ServerName>>(tableRegionsAndLocations.size());
    Pair<byte[], ServerName> startKeyAndLocation = null;
    for (Pair<HRegionInfo, ServerName> regionAndLocation : tableRegionsAndLocations) {
      startKeyAndLocation =
          new Pair<byte[], ServerName>(regionAndLocation.getFirst().getStartKey(),
              regionAndLocation.getSecond());
      startKeyAndLocationPairs.add(startKeyAndLocation);
    }
    return startKeyAndLocationPairs;

  }

  private boolean checkForColocation(HMaster master, String tableName, String indexTableName)
      throws IOException, InterruptedException {
    List<Pair<byte[], ServerName>> uTableStartKeysAndLocations =
        getStartKeysAndLocations(master, tableName);
    List<Pair<byte[], ServerName>> iTableStartKeysAndLocations =
        getStartKeysAndLocations(master, indexTableName);

    boolean regionsColocated = true;
    if (uTableStartKeysAndLocations.size() != iTableStartKeysAndLocations.size()) {
      regionsColocated = false;
    } else {
      for (int i = 0; i < uTableStartKeysAndLocations.size(); i++) {
        Pair<byte[], ServerName> uStartKeyAndLocation = uTableStartKeysAndLocations.get(i);
        Pair<byte[], ServerName> iStartKeyAndLocation = iTableStartKeysAndLocations.get(i);

        if (Bytes.compareTo(uStartKeyAndLocation.getFirst(), iStartKeyAndLocation.getFirst()) == 0) {
          if (uStartKeyAndLocation.getSecond().equals(iStartKeyAndLocation.getSecond())) {
            continue;
          }
        }
        regionsColocated = false;
      }
    }
    return regionsColocated;
  }

}
