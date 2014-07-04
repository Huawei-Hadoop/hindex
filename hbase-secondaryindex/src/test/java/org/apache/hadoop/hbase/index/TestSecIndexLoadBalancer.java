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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
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
  
  private static HBaseAdmin admin = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 4;
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,true);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    admin = new IndexAdmin(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if(admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentDuringIndexTableCreation() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    String tableName = "testRoundRobinAssignmentDuringIndexTableCreation";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
  }

  @Test(timeout = 180000)
  public void testColocationWhenNormalTableModifiedToIndexedTable() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    String tableName = "testColocationWhenNormalTableModifiedToIndexedTable";
    HMaster master = cluster.getMaster();
    byte[][] split = new byte[20][];
    char c = 'A';
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    admin.createTable(htd, split);
    IndexSpecification iSpec = new IndexSpecification("index_name");
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    iSpec.addIndexColumn(hcd, "cq", ValueType.String, 10);
    htd.setValue(Constants.INDEX_SPEC_KEY, tableIndices.toByteArray());
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    admin.disableTable(tableName);
    admin.modifyTable(tableName, htd);
    admin.enableTable(tableName);
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
  }

  @Test(timeout = 180000)
  public void testColocationAfterSplit() throws Exception {
    String userTableName = "testCompactionOnIndexTableShouldNotRetrieveTTLExpiredData";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd = new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE);

    HColumnDescriptor hcd1 = new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, tableIndices.toByteArray());
    admin.createTable(ihtd);
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
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
    admin.flush(indexTableName.getNameAsString());

    admin.split(userTableName, "row31");
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    List<HRegionInfo> regionsOfUserTable =
        master.getAssignmentManager().getRegionStates().getRegionsOfTable(TableName.valueOf(userTableName));

    while (regionsOfUserTable.size() != 2) {
      Thread.sleep(100);
      regionsOfUserTable =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(TableName.valueOf(userTableName));
    }
    
    List<HRegionInfo> regionsOfIndexTable =
        master.getAssignmentManager().getRegionStates().getRegionsOfTable(indexTableName);

    while (regionsOfIndexTable.size() != 2) {
      Thread.sleep(100);
      regionsOfIndexTable =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(indexTableName);
    }

    for (HRegionInfo hRegionInfo : regionsOfUserTable) {
      for (HRegionInfo indexRegionInfo : regionsOfIndexTable) {
        if (Bytes.equals(hRegionInfo.getStartKey(), indexRegionInfo.getStartKey())) {
          assertEquals("The regions should be colocated", master.getAssignmentManager()
              .getRegionStates().getRegionServerOfRegion(hRegionInfo), master.getAssignmentManager()
              .getRegionStates().getRegionServerOfRegion(indexRegionInfo));
        }
      }
    }

  }

  @Test(timeout = 180000)
  public void testRandomAssignmentDuringIndexTableEnable() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
    HTableDescriptor iHtd =
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
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testBalanceCluster() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);
    master.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", false);
    String tableName = "testBalanceCluster";

    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor(tableName, "cf", "index_name", "cf", "cq");
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testBalanceCluster1"));
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
    admin.setBalancerRunning(false, false);
    admin.createTable(htd, split1);
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    admin.setBalancerRunning(true, false);
    admin.balancer();
    boolean isRegionsColocated =
        TestUtils.checkForColocation(master, tableName, IndexUtils.getIndexTableName(tableName));
    assertTrue("User regions and index regions should colocate.", isRegionsColocated);
  }

  @Test//(timeout = 180000)
  public void testBalanceByTable() throws Exception {
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", true);
    admin.setBalancerRunning(false, false);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testBalanceByTable", "cf",
          "index_name", "cf", "cq");
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testBalanceByTable1"));
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
    admin.createTable(htd, split1);
    String tableName = "testBalanceByTable";
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    admin.disableTable(tableName);
    admin.enableTable(tableName);
    admin.setBalancerRunning(true, false);
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
    admin.balancer();
    Thread.sleep(10000);
    ZKAssign.blockUntilNoRIT(zkw);
    master.getAssignmentManager().waitUntilNoRegionsInTransition(10000);
    isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);
  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentAfterRegionServerDown() throws Exception {
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRoundRobinAssignmentAfterRegionServerDown", "cf",
          "index_name", "cf", "cq");
    char c = 'A';
    byte[][] split = new byte[20][];
    for (int i = 0; i < 20; i++) {
      byte[] b = { (byte) c };
      split[i] = b;
      c++;
    }
    admin.createTable(iHtd, split);

    String tableName = "testRoundRobinAssignmentAfterRegionServerDown";
    String indexTableName =
        "testRoundRobinAssignmentAfterRegionServerDown" + Constants.INDEX_TABLE_SUFFIX;
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
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testRegionMove() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    TableName tableName = TableName.valueOf("testRegionMove");
    HTableDescriptor iHtd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    iHtd.addFamily(hcd);
    iHtd.setValue(Constants.INDEX_SPEC_KEY, tableIndices.toByteArray());
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
          iHtd.getTableName());
    int numRegions = cluster.getRegionServerThreads().size();
    cluster.getRegionServer(1).getServerName();
    Random random = new Random();
    for (HRegionInfo hRegionInfo : tableRegions) {
      int regionNumber = random.nextInt(numRegions);
      ServerName serverName = cluster.getRegionServer(regionNumber).getServerName();
      admin.move(hRegionInfo.getEncodedNameAsBytes(), Bytes.toBytes(serverName.getServerName()));
    }
    ZKAssign.blockUntilNoRIT(zkw);
    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    while(!am.waitUntilNoRegionsInTransition(1000));
    boolean isRegionColocated =
        TestUtils.checkForColocation(master, tableName.getNameAsString(),
          IndexUtils.getIndexTableName(tableName));
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test//(timeout = 180000)
  public void testRetainAssignmentDuringMasterStartUp() throws Exception {
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", true);
    HTableDescriptor iHtd =
        TestUtils.createIndexedHTableDescriptor("testRetainAssignmentDuringMasterStartUp", "cf",
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
    UTIL.shutdownMiniHBaseCluster();
    UTIL.startMiniHBaseCluster(1, 4);
    cluster = UTIL.getHBaseCluster();
    master = cluster.getMaster();
    ZKAssign.blockUntilNoRIT(zkw);
    if (admin != null) {
      admin.close();
      admin = new IndexAdmin(cluster.getMaster().getConfiguration());
    }
    master.getAssignmentManager().waitUntilNoRegionsInTransition(1000);
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  @Test(timeout = 180000)
  public void testRoundRobinAssignmentDuringMasterStartUp() throws Exception {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);

    HTableDescriptor iHtd =
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
    UTIL.shutdownMiniHBaseCluster();
    UTIL.startMiniHBaseCluster(1, 4);
    cluster = UTIL.getHBaseCluster();
    if (admin != null) {
      admin.close();
      admin = new IndexAdmin(cluster.getMaster().getConfiguration());
    }
    master = cluster.getMaster();
    boolean isRegionColocated = TestUtils.checkForColocation(master, tableName, indexTableName);
    assertTrue("User regions and index regions should colocate.", isRegionColocated);

  }

  private List<Pair<byte[], ServerName>> getStartKeysAndLocations(HMaster master, String tableName)
      throws IOException, InterruptedException {

    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations =
        MetaReader.getTableRegionsAndLocations(master.getCatalogTracker(),
          TableName.valueOf(tableName));
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

}
