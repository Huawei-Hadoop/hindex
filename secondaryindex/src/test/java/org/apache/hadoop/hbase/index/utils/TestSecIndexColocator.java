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
package org.apache.hadoop.hbase.index.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.SecondaryIndexColocator;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void finishAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testCoLocationFixing() throws Exception {
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    Configuration config = UTIL.getConfiguration();

    String userTableName = "testCoLocationFixing";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("cf1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("idx");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    ihtd.addIndex(iSpec);

    char c = 'A';
    byte[][] splits = new byte[5][];
    for (int i = 0; i < 5; i++) {
      byte[] b = { (byte) c };
      c++;
      splits[i] = b;
    }
    admin.createTable(ihtd, splits);

    String userTableName1 = "testCoLocationFixing1";
    ihtd = new IndexedHTableDescriptor(userTableName1);
    ihtd.addFamily(hcd);
    iSpec = new IndexSpecification("idx1");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd, splits);

    String userTableName2 = "testCoLocationFixing2";
    ihtd = new IndexedHTableDescriptor(userTableName2);
    ihtd.addFamily(hcd);
    iSpec = new IndexSpecification("idx2");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd, splits);

    List<byte[]> regions = UTIL.getMetaTableRows(Bytes.toBytes(userTableName + "_idx"));
    List<byte[]> regionsEncod = getEncodedNames(regions);

    List<byte[]> regions1 = UTIL.getMetaTableRows(Bytes.toBytes(userTableName1 + "_idx"));
    List<byte[]> regionsEncod1 = getEncodedNames(regions1);

    List<byte[]> regions2 = UTIL.getMetaTableRows(Bytes.toBytes(userTableName2 + "_idx"));
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
    Assert.assertTrue("Inconsistency should be there before running the tool.", inconsistent);
    colocator.fixCoLocationInconsistency();

    ZKAssign.blockUntilNoRIT(zkw);

    colocator = new SecondaryIndexColocator(config);
    colocator.setUp();
    inconsistent = colocator.checkForCoLocationInconsistency();
    Assert.assertFalse("No inconsistency should be there after running the tool", inconsistent);
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(table);
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
    admin.disableTable(table);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));
    byte[][] splits2 = new byte[11][];
    c = 'A';
    splits2[0] = new byte[0];
    for (int i = 1; i < 11; i++) {
      byte[] b = { (byte) c };
      splits2[i] = b;
      c++;
    }

    FileSystem filesystem = FileSystem.get(UTIL.getConfiguration());
    Path rootdir =
        filesystem.makeQualified(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    HMaster master = UTIL.getMiniHBaseCluster().getMasterThreads().get(0).getMaster();
    String table2 = "testWhenUserTableIsDisabledButIndexTableIsInDisablingState_idx";
    HTableDescriptor htd2 = new HTableDescriptor(table2);
    htd2.addFamily(new HColumnDescriptor(new String("cf1")));
    FSTableDescriptors.createTableDescriptor(htd2, UTIL.getConfiguration());
    List<HRegionInfo> regionsInMeta =
        UTIL.createMultiRegionsInMeta(UTIL.getConfiguration(), htd2, splits2);
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo regionInfo : regionsInMeta) {
      HRegion r = HRegion.createHRegion(regionInfo, rootdir, UTIL.getConfiguration(), htd2);
      newRegions.add(r.getRegionInfo());
    }

    for (int i = 0; i < newRegions.size() / 2; i++) {
      admin.assign(newRegions.get(i).getRegionName());
    }
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));
    master.getAssignmentManager().getZKTable().setDisablingTable(table2);

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
      onlineregions.addAll(hrs.getOnlineRegions());
    }

    boolean regionOnline = false;
    for (HRegionInfo hri : onlineregions) {
      for (HRegionInfo disabledregion : newRegions) {
        if (hri.equals(disabledregion)) {
          regionOnline = true;
          break;
        }
      }
    }
    Assert.assertFalse("NO region from the disabledTable should be online.", regionOnline);
    Assert.assertTrue("The disabling table should be now disabled",
      ZKTableReadOnly.isDisabledTable(HBaseTestingUtility.getZooKeeperWatcher(UTIL), table2));
  }

  @Test(timeout = 180000)
  public void testWhenUserTableIsDisabledButIndexTableIsInEnabledState() throws Exception {
    String table = "testWhenUserTableIsDisabledButIndexTableIsInEnabledState";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(table);
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
    admin.disableTable(table);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(UTIL));
    byte[][] splits2 = new byte[11][];
    c = 'A';
    splits2[0] = new byte[0];
    for (int i = 1; i < 11; i++) {
      byte[] b = { (byte) c };
      splits2[i] = b;
      c++;
    }

    FileSystem filesystem = FileSystem.get(UTIL.getConfiguration());
    Path rootdir =
        filesystem.makeQualified(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    HMaster master = UTIL.getMiniHBaseCluster().getMasterThreads().get(0).getMaster();
    String table2 = "testWhenUserTableIsDisabledButIndexTableIsInEnabledState_idx";
    HTableDescriptor htd2 = new HTableDescriptor(table2);
    htd2.addFamily(new HColumnDescriptor(new String("cf1")));
    FSTableDescriptors.createTableDescriptor(htd2, UTIL.getConfiguration());
    List<HRegionInfo> regionsInMeta =
        UTIL.createMultiRegionsInMeta(UTIL.getConfiguration(), htd2, splits2);
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo regionInfo : regionsInMeta) {
      HRegion r = HRegion.createHRegion(regionInfo, rootdir, UTIL.getConfiguration(), htd2);
      newRegions.add(r.getRegionInfo());
    }

    for (int i = 0; i < newRegions.size() / 2; i++) {
      admin.assign(newRegions.get(i).getRegionName());
    }
    master.getAssignmentManager().getZKTable().setEnabledTable(table2);

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
      onlineregions.addAll(hrs.getOnlineRegions());
    }

    boolean regionOnline = false;
    for (HRegionInfo hri : onlineregions) {
      for (HRegionInfo disabledregion : newRegions) {
        if (hri.equals(disabledregion)) {
          regionOnline = true;
        }
      }
    }
    Assert.assertFalse("NO region from the disabledTable should be online.", regionOnline);
    Assert.assertTrue("The enabled table should be now disabled",
      ZKTableReadOnly.isDisabledTable(HBaseTestingUtility.getZooKeeperWatcher(UTIL), table2));
  }

  @Test(timeout = 180000)
  public void testWhenAllUSerRegionsAreAssignedButNotSameForIndex() throws Exception {
    String table = "testWhenAllUSerRegionsAreAssignedButNotSameForIndex";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(table);
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

    FileSystem filesystem = FileSystem.get(UTIL.getConfiguration());
    Path rootdir =
        filesystem.makeQualified(new Path(UTIL.getConfiguration().get(HConstants.HBASE_DIR)));
    HMaster master = UTIL.getMiniHBaseCluster().getMasterThreads().get(0).getMaster();
    String table2 = "testWhenAllUSerRegionsAreAssignedButNotSameForIndex_idx";
    HTableDescriptor htd2 = new HTableDescriptor(table2);
    htd2.addFamily(new HColumnDescriptor(new String("cf1")));
    FSTableDescriptors.createTableDescriptor(htd2, UTIL.getConfiguration());
    List<HRegionInfo> regionsInMeta =
        UTIL.createMultiRegionsInMeta(UTIL.getConfiguration(), htd2, splits2);
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo regionInfo : regionsInMeta) {
      HRegion r = HRegion.createHRegion(regionInfo, rootdir, UTIL.getConfiguration(), htd2);
      newRegions.add(r.getRegionInfo());
    }

    for (int i = 0; i < newRegions.size() / 2; i++) {
      admin.assign(newRegions.get(i).getRegionName());
    }
    master.getAssignmentManager().getZKTable().setEnabledTable(table2);

    SecondaryIndexColocator colocator = new SecondaryIndexColocator(UTIL.getConfiguration());
    colocator.setUp();
    colocator.checkForCoLocationInconsistency();
    List<RegionServerThread> serverThreads =
        UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    List<HRegionServer> rs = new ArrayList<HRegionServer>();
    for (RegionServerThread regionServerThread : serverThreads) {
      rs.add(regionServerThread.getRegionServer());
    }

    Set<HRegionInfo> onlineregions = new HashSet<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      onlineregions.addAll(hrs.getOnlineRegions());
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : newRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    Assert.assertFalse("All region from the index Table should be online.", regionOffline);
  }

  @Test(timeout = 180000)
  public void testWhenUserTableIsEabledButIndexTableIsDisabled() throws Exception {
    String table = "testWhenUserTableIsEabledButIndexTableIsDisabled";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(table);
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
    HTableDescriptor htd2 = new HTableDescriptor(table2);
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

    Set<HRegionInfo> onlineregions = new HashSet<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      onlineregions.addAll(hrs.getOnlineRegions());
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : tableRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    Assert.assertFalse("All region from the disabledTable should be online.", regionOffline);
  }

  @Test(timeout = 180000)
  public void testWhenRegionsAreNotAssignedAccordingToMeta() throws Exception {
    String table = "testWhenRegionsAreNotAssignedAccordingToMeta";
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(table);
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

    ServerName sn = new ServerName("example.org", 1234, 5678);
    HMaster master = UTIL.getMiniHBaseCluster().getMaster(0);

    List<HRegionInfo> tableRegions = admin.getTableRegions(Bytes.toBytes(table));

    for (int i = 0; i < 5; i++) {
      MetaEditor.updateRegionLocation(master.getCatalogTracker(), tableRegions.get(i), sn);
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

    Set<HRegionInfo> onlineregions = new HashSet<HRegionInfo>();
    for (HRegionServer hrs : rs) {
      onlineregions.addAll(hrs.getOnlineRegions());
    }

    boolean regionOffline = false;
    for (HRegionInfo hri : tableRegions) {
      if (!onlineregions.contains(hri)) {
        regionOffline = true;
        break;
      }
    }
    Assert.assertFalse(
      "All the regions with wrong META info should be assiged to some online server.",
      regionOffline);
  }
}
