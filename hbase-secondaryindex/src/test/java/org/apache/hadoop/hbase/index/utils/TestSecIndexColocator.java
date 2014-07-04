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
package org.apache.hadoop.hbase.index.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.index.util.SecondaryIndexColocator;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSecIndexColocator {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  
  private static HBaseAdmin admin = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    UTIL.startMiniCluster(2);
    admin = new IndexAdmin(conf);
  }

  @AfterClass
  public static void finishAfterClass() throws Exception {
    if(admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testCoLocationFixing() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    Configuration config = UTIL.getConfiguration();
    String userTableName = "testCoLocationFixing";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "cf1", "idx", "cf1", "q");
    char c = 'A';
    byte[][] splits = new byte[5][];
    for (int i = 0; i < 5; i++) {
      byte[] b = { (byte) c };
      c++;
      splits[i] = b;
    }
    admin.createTable(ihtd, splits);

    String userTableName1 = "testCoLocationFixing1";
    
    ihtd = TestUtils.createIndexedHTableDescriptor(userTableName1, "cf1", "idx1", "cf1", "q");
    admin.createTable(ihtd, splits);

    String userTableName2 = "testCoLocationFixing2";
    ihtd = TestUtils.createIndexedHTableDescriptor(userTableName2, "cf1", "idx2", "cf1", "q");

    admin.createTable(ihtd, splits);
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    List<byte[]> regions = UTIL.getMetaTableRows(indexTableName);
    List<byte[]> regionsEncod = getEncodedNames(regions);

    List<byte[]> regions1 = UTIL.getMetaTableRows(indexTableName);
    List<byte[]> regionsEncod1 = getEncodedNames(regions1);

    List<byte[]> regions2 = UTIL.getMetaTableRows(indexTableName);
    List<byte[]> regionsEncod2 = getEncodedNames(regions2);

    for (int i = 0; i < 2; i++) {
      admin.move(regionsEncod.get(i), null);
      admin.move(regionsEncod1.get(i), null);
      admin.move(regionsEncod2.get(i), null);
    }

    ZKAssign.blockUntilNoRIT(zkw);
    SecondaryIndexColocator colocator = new SecondaryIndexColocator(config);
    colocator.setUp();
    boolean inconsistent = colocator.checkForCoLocationInconsistency();
    assertTrue("Inconsistency should be there before running the tool.", inconsistent);
    colocator.fixCoLocationInconsistency();

    ZKAssign.blockUntilNoRIT(zkw);

    colocator = new SecondaryIndexColocator(config);
    colocator.setUp();
    inconsistent = colocator.checkForCoLocationInconsistency();
    assertFalse("No inconsistency should be there after running the tool", inconsistent);
  }

  private List<byte[]> getEncodedNames(List<byte[]> regions) {
    List<byte[]> regionsEncod = new ArrayList<byte[]>();
    for (byte[] r : regions) {
      String rs = Bytes.toString(r);
      int firstOcc = rs.indexOf('.');
      int lastOcc = rs.lastIndexOf('.');
      regionsEncod.add(Bytes.toBytes(rs.substring(firstOcc + 1, lastOcc)));
    }
    return regionsEncod;
  }

  @Test(timeout = 180000)
  public void testWhenUserTableIsDisabledButIndexTableIsInDisablingState() throws Exception {
    String table = "testWhenUserTableIsDisabledButIndexTableIsInDisablingState";
    HTableDescriptor htd =
        TestUtils.createIndexedHTableDescriptor(table, "cf", "index_name", "cf", "cq");
    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(htd, splits);
    admin.disableTable(table);
    byte[][] splits2 = new byte[11][];
    c = 'A';
    splits2[0] = new byte[0];
    for (int i = 1; i < 11; i++) {
      byte[] b = { (byte) c };
      splits2[i] = b;
      c++;
    }

    HMaster master = UTIL.getMiniHBaseCluster().getMasterThreads().get(0).getMaster();
    TableName tableName2 = TableName.valueOf("testWhenUserTableIsDisabledButIndexTableIsInDisablingState_idx");
    master.getAssignmentManager().getZKTable().setDisablingTable(tableName2);

    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    boolean inconsistent = colocator.checkForCoLocationInconsistency();
    assertTrue("The disabling table should be now disabled",
      ZKTableReadOnly.isDisabledTable(HBaseTestingUtility.getZooKeeperWatcher(UTIL), tableName2));
  }

  @Test(timeout = 180000)
  public void testWhenUserTableIsDisabledButIndexTableIsInEnabledState() throws Exception {
    String table = "testWhenUserTableIsDisabledButIndexTableIsInEnabledState";
    HTableDescriptor htd =
        TestUtils.createIndexedHTableDescriptor(table, "cf", "index_name", "cf", "cq");
    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(htd, splits);
    admin.disableTable(table);
    admin.enableTable(IndexUtils.getIndexTableName(table));
    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    colocator.checkForCoLocationInconsistency();
    assertTrue(
      "The enabled table should be now disabled",
      ZKTableReadOnly.isDisabledTable(HBaseTestingUtility.getZooKeeperWatcher(UTIL),
        TableName.valueOf(IndexUtils.getIndexTableName(table))));
  }

  @Test//(timeout = 180000)
  public void testWhenAllUSerRegionsAreAssignedButNotSameForIndex() throws Exception {
    String table = "testWhenAllUSerRegionsAreAssignedButNotSameForIndex";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    htd.addFamily(new HColumnDescriptor(new String("cf")));
    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(htd, splits);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));

    byte[][] splits2 = new byte[11][];
    c = 'A';
    splits2[0] = new byte[0];
    for (int i = 1; i < 11; i++) {
      byte[] b = { (byte) c };
      splits2[i] = b;
      c++;
    }

    Configuration conf = UTIL.getConfiguration();
    HMaster master = UTIL.getMiniHBaseCluster().getMasterThreads().get(0).getMaster();
    String table2 = "testWhenAllUSerRegionsAreAssignedButNotSameForIndex_idx";
    TableName tableName2 = TableName.valueOf(table2);
    HTableDescriptor htd2 = new HTableDescriptor(tableName2);
    htd2.addFamily(new HColumnDescriptor(new String("cf1")));
    MasterFileSystem fileSystemManager = master.getMasterFileSystem();
    // 1. Create Table Descriptor
    Path tempTableDir = FSUtils.getTableDir(fileSystemManager.getTempDir(), tableName2);
    new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(
      tempTableDir, htd2, false);
    Path tableDir = FSUtils.getTableDir(fileSystemManager.getRootDir(), tableName2);
    if (!fileSystemManager.getFileSystem().rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }
    List<HRegionInfo> regionsInMeta =
        UTIL.createMultiRegionsInMeta(UTIL.getConfiguration(), htd2, splits2);
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>();
    for (int i = 0; i < regionsInMeta.size() / 2; i++) {
      admin.assign(regionsInMeta.get(i).getRegionName());
    }
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));
    master.getAssignmentManager().getZKTable().setEnabledTable(tableName2);

    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    colocator.checkForCoLocationInconsistency();
    List<RegionServerThread> serverThreads =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    List<HRegionServer> rs = new ArrayList<HRegionServer>();
    for (RegionServerThread regionServerThread : serverThreads) {
      rs.add(regionServerThread.getRegionServer());
    }

    List<HRegionInfo> onlineregions = new ArrayList<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      List<HRegion> regions = hrs.getOnlineRegions(tableName2);
      for (HRegion region : regions) {
        onlineregions.add(region.getRegionInfo());
      }
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : newRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    assertFalse("All region from the index Table should be online.", regionOffline);
  }

  @Test(timeout = 180000)
  public void testWhenUserTableIsEabledButIndexTableIsDisabled() throws Exception {
    String table = "testWhenUserTableIsEabledButIndexTableIsDisabled";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    htd.addFamily(new HColumnDescriptor(new String("cf")));
    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(htd, splits);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));

    String table2 = "testWhenUserTableIsEabledButIndexTableIsDisabled_idx";
    TableName tableName2 = TableName.valueOf(table2);
    HTableDescriptor htd2 = new HTableDescriptor(tableName2);
    htd2.addFamily(new HColumnDescriptor(new String("cf")));
    admin.createTable(htd2, splits);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));
    admin.disableTable(table2);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));

    List<HRegionInfo> tableRegions = admin.getTableRegions(Bytes.toBytes(table2));
    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    boolean inconsistent = colocator.checkForCoLocationInconsistency();
    List<RegionServerThread> serverThreads =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();

    List<HRegionServer> rs = new ArrayList<HRegionServer>();
    for (RegionServerThread regionServerThread : serverThreads) {
      rs.add(regionServerThread.getRegionServer());
    }

    List<HRegionInfo> onlineregions = new ArrayList<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      List<HRegion> regions = hrs.getOnlineRegions(tableName2);
      for (HRegion region : regions) {
        onlineregions.add(region.getRegionInfo());
      }
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : tableRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    assertFalse("All region from the disabledTable should be online.", regionOffline);
  }

  @Test(timeout = 180000)
  public void testWhenRegionsAreNotAssignedAccordingToMeta() throws Exception {
    String table = "testWhenRegionsAreNotAssignedAccordingToMeta";
    TableName tableName = TableName.valueOf(table);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(new String("cf")));
    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(htd, splits);

    ServerName sn = ServerName.valueOf("example.org", 1234, 5678);
    HMaster master = UTIL.getMiniHBaseCluster().getMaster(0);

    List<HRegionInfo> tableRegions = admin.getTableRegions(Bytes.toBytes(table));
    List<HRegion> hRegions = UTIL.getMiniHBaseCluster().getRegions(Bytes.toBytes(table));
    for (int i = 0; i < 5; i++) {
      MetaEditor.updateRegionLocation(master.getCatalogTracker(), tableRegions.get(i), sn, hRegions
          .get(i).getOpenSeqNum());
    }

    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    colocator.checkForCoLocationInconsistency();

    List<RegionServerThread> serverThreads =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    List<HRegionServer> rs = new ArrayList<HRegionServer>();
    for (RegionServerThread regionServerThread : serverThreads) {
      rs.add(regionServerThread.getRegionServer());
    }

    List<HRegionInfo> onlineregions = new ArrayList<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      List<HRegion> regions = hrs.getOnlineRegions(tableName);
      for (HRegion region : regions) {
        onlineregions.add(region.getRegionInfo());
      }
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : tableRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    assertFalse("All the regions with wrong META info should be assiged to some online server.",
      regionOffline);
  }
}
