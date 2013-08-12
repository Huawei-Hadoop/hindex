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
package org.apache.hadoop.hbase.index.coprocessor.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestForComplexIssues {
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  public static CountDownLatch latch = new CountDownLatch(1);
  public static CountDownLatch latchForCompact = new CountDownLatch(1);
  private static boolean compactionCalled = false;
  private static volatile boolean closeCalled = false;
  private static volatile boolean delayPostBatchMutate = false;
  private static int openCount = 0;
  private static int closeCount = 0;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, LocalIndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);

  }

  @Before
  public void setUp() throws Exception {
    UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    compactionCalled = false;
    closeCalled = false;
    delayPostBatchMutate = false;
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testHDP2989() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.client.retries.number", 1);
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setInt("hbase.regionserver.lease.period", 90000000);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testHDP2989";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd);
    ZKAssign.blockUntilNoRIT(zkw);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
      admin.flush(userTableName);
    }

    MiniHBaseCluster hBaseCluster = UTIL.getHBaseCluster();
    List<HRegion> regions = hBaseCluster.getRegions(Bytes.toBytes(userTableName));
    HRegion hRegion = regions.get(0);
    byte[] encodedNameAsBytes = hRegion.getRegionInfo().getEncodedNameAsBytes();
    int serverWith = hBaseCluster.getServerWith(hRegion.getRegionName());
    int destServerNo = (serverWith == 1 ? 0 : 1);
    HRegionServer destServer = hBaseCluster.getRegionServer(destServerNo);

    admin.compact(userTableName);

    while (!compactionCalled) {
      Thread.sleep(1000);
    }

    byte[] dstRSName = Bytes.toBytes(destServer.getServerName().getServerName());
    // Move the main region
    MoveThread move = new MoveThread(admin, encodedNameAsBytes, dstRSName);
    move.start();

    // Move the indexRegion
    regions = hBaseCluster.getRegions(Bytes.toBytes(userTableName + "_idx"));
    HRegion hRegion1 = regions.get(0);
    encodedNameAsBytes = hRegion1.getRegionInfo().getEncodedNameAsBytes();
    serverWith = hBaseCluster.getServerWith(hRegion1.getRegionName());
    destServerNo = (serverWith == 1 ? 0 : 1);
    destServer = hBaseCluster.getRegionServer(destServerNo);

    dstRSName = Bytes.toBytes(destServer.getServerName().getServerName());
    move = new MoveThread(admin, encodedNameAsBytes, dstRSName);
    move.start();
    while (!closeCalled) {
      Thread.sleep(200);
    }

    String row = "row" + 46;
    Put p = new Put(row.getBytes());
    String val = "Val" + 46;
    p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
    try {
      table.put(p);
    } catch (Exception e) {
      e.printStackTrace();
    }
    latchForCompact.countDown();
    closeCount++;
    latch.countDown();
    List<HRegion> onlineRegions = destServer.getOnlineRegions(Bytes.toBytes(userTableName));
    List<HRegion> onlineIdxRegions =
        destServer.getOnlineRegions(Bytes.toBytes(userTableName + "_idx"));
    while (onlineRegions.size() == 0 || onlineIdxRegions.size() == 0) {
      Thread.sleep(1000);
      onlineRegions = destServer.getOnlineRegions(Bytes.toBytes(userTableName));
      onlineIdxRegions = destServer.getOnlineRegions(Bytes.toBytes(userTableName + "_idx"));
    }

    /*
     * closeCount++; latch.countDown();
     */

    Scan s = new Scan();
    conf.setInt("hbase.client.retries.number", 10);
    table = new HTable(conf, userTableName);
    ResultScanner scanner = table.getScanner(s);
    int i = 0;
    for (Result result : scanner) {
      i++;
    }
    HTable indextable = new HTable(conf, userTableName + "_idx");
    s = new Scan();
    scanner = indextable.getScanner(s);
    int j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", i, j);

  }

  @Test(timeout = 180000)
  // test for HDP-2983
      public
      void testCompactionOfIndexRegionBeforeMainRegionOpens() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    Configuration conf = admin.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    String userTableName = "testCompactionOfIndexRegionBeforeMainRegionOpens";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    IndexSpecification iSpec1 = new IndexSpecification("Index2");
    iSpec1.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addIndex(iSpec);
    ihtd.addIndex(iSpec1);
    admin.createTable(ihtd);

    ZKAssign.blockUntilNoRIT(zkw);
    openCount++;
    HTable table = new HTable(conf, userTableName);
    Put p = null;
    for (int i = 0; i < 25; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql"), Bytes.toBytes("test_val"));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("q2"), Bytes.toBytes("test_val"));
      table.put(p);
      if (i == 4 || i == 11 || i == 24) {
        admin.flush(userTableName);
        admin.flush(userTableName + "_idx");
      }
    }

    HTable indextable = new HTable(conf, userTableName + "_idx");
    Scan s = new Scan();
    ResultScanner scanner = indextable.getScanner(s);
    int j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", 50, j);

    MiniHBaseCluster hBaseCluster = UTIL.getHBaseCluster();
    List<HRegion> regions = hBaseCluster.getRegions(Bytes.toBytes(userTableName));
    HRegion hRegion = regions.get(0);

    List<HRegion> indexRegions = hBaseCluster.getRegions(Bytes.toBytes(userTableName + "_idx"));
    HRegion indexHRegion = indexRegions.get(0);
    int srcServer = hBaseCluster.getServerWith(hRegion.getRegionName());
    int destServer = (srcServer == 1 ? 0 : 1);
    HRegionServer regionServer = hBaseCluster.getRegionServer(destServer);

    // Abort the regionserver
    hBaseCluster.abortRegionServer(srcServer);
    hBaseCluster.waitOnRegionServer(srcServer);

    boolean regionOnline =
        hBaseCluster.getMaster().getAssignmentManager()
            .isRegionAssigned(indexHRegion.getRegionInfo());
    while (!regionOnline) {
      regionOnline =
          hBaseCluster.getMaster().getAssignmentManager()
              .isRegionAssigned(indexHRegion.getRegionInfo());
    }
    admin.compact(userTableName + "_idx");
    List<HRegion> onlineRegions =
        regionServer.getOnlineRegions(Bytes.toBytes(userTableName + "_idx"));
    List<String> storeFileList =
        regionServer.getStoreFileList(onlineRegions.get(0).getRegionName());
    while (storeFileList.size() != 1) {
      Thread.sleep(1000);
      storeFileList = regionServer.getStoreFileList(onlineRegions.get(0).getRegionName());
    }
    assertEquals("The total store files for the index table should be 1", 1, storeFileList.size());

    indextable = new HTable(conf, userTableName + "_idx");
    s = new Scan();
    scanner = indextable.getScanner(s);
    j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", 50, j);
    // Decrement the latch
    latch.countDown();
    ZKAssign.blockUntilNoRIT(zkw);
    openCount = 0;
    // Check after dropping the index
    storeFileList = regionServer.getStoreFileList(onlineRegions.get(0).getRegionName());
    String storeFileName = storeFileList.get(0);
    // admin.dropIndex("Index2", userTableName);
    ZKAssign.blockUntilNoRIT(zkw);
    while (storeFileList.get(0).equals(storeFileName)) {
      Thread.sleep(1000);
      storeFileList = regionServer.getStoreFileList(onlineRegions.get(0).getRegionName());
    }

    indextable = new HTable(conf, userTableName + "_idx");
    s = new Scan();
    scanner = indextable.getScanner(s);
    j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", 25, j);

  }

  @Test(timeout = 180000)
  public void testHDP3015() throws Exception {
    final HBaseAdmin admin = UTIL.getHBaseAdmin();
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setInt("hbase.regionserver.lease.period", 90000000);
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(UTIL);
    final String userTableName = "testHDP3015";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(userTableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd);
    ZKAssign.blockUntilNoRIT(zkw);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 4; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      if (i == 3) {
        delayPostBatchMutate = true;
      }
      table.put(p);
      if (i == 2) {
        new Thread() {
          public void run() {
            try {
              admin.flush(userTableName + "_idx");
            } catch (Exception e) {

            }
          }
        }.start();
      }
    }
    MiniHBaseCluster hBaseCluster = UTIL.getHBaseCluster();
    List<HRegion> userRegions = hBaseCluster.getRegions(Bytes.toBytes(userTableName));

    List<HRegion> indexRegions = hBaseCluster.getRegions(Bytes.toBytes(userTableName + "_idx"));
    HRegion indexHRegion = indexRegions.get(0);

    int serverWith = hBaseCluster.getServerWith((userRegions.get(0).getRegionName()));
    HRegionServer regionServer = hBaseCluster.getRegionServer(serverWith);
    List<String> storeFileList = regionServer.getStoreFileList(indexHRegion.getRegionName());
    while (storeFileList.size() == 0) {
      storeFileList = regionServer.getStoreFileList(indexHRegion.getRegionName());
    }

    table = new HTable(conf, userTableName);
    ResultScanner scanner = table.getScanner(new Scan());
    int i = 0;
    for (Result result : scanner) {
      i++;
    }
    table = new HTable(conf, userTableName + "_idx");
    scanner = table.getScanner(new Scan());
    int j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", i, j);

    hBaseCluster.abortRegionServer(serverWith);
    hBaseCluster.waitOnRegionServer(serverWith);

    boolean regionOnline =
        hBaseCluster.getMaster().getAssignmentManager()
            .isRegionAssigned(indexHRegion.getRegionInfo());
    while (!regionOnline) {
      regionOnline =
          hBaseCluster.getMaster().getAssignmentManager()
              .isRegionAssigned(indexHRegion.getRegionInfo());
    }

    table = new HTable(conf, userTableName);
    scanner = table.getScanner(new Scan());
    i = 0;
    for (Result result : scanner) {
      i++;
    }
    table = new HTable(conf, userTableName + "_idx");
    scanner = table.getScanner(new Scan());
    j = 0;
    for (Result result : scanner) {
      j++;
    }
    assertEquals("", i, j);

  }

  public static class LocalIndexRegionObserver extends IndexRegionObserver {

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
        Store store, List<? extends KeyValueScanner> scanners, ScanType scanType,
        long earliestPutTs, InternalScanner s) throws IOException {
      if (store.getTableName().equals("testHDP2989")) {
        try {
          compactionCalled = true;
          latchForCompact.await();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
      if (e.getEnvironment().getRegion().getRegionInfo().getTableNameAsString()
          .equals("testCompactionOfIndexRegionBeforeMainRegionOpens")
          && openCount > 0) {
        try {
          latch.await();
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      super.postOpen(e);
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
      if (e.getEnvironment().getRegion().getRegionInfo().getTableNameAsString()
          .equals("testHDP2989_idx")
          && closeCount == 0) {
        try {
          closeCalled = true;
          latch.await();
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }
      super.postClose(e, abortRequested);
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> ctx,
        List<Mutation> mutations, WALEdit walEdit) {
      if (ctx.getEnvironment().getRegion().getRegionInfo().getTableNameAsString()
          .contains("testHDP3015")) {
        if (delayPostBatchMutate) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        super.postBatchMutate(ctx, mutations, walEdit);
      } else {
        super.postBatchMutate(ctx, mutations, walEdit);
      }
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
      if (e.getEnvironment().getRegion().getRegionInfo().getTableNameAsString()
          .contains("testHDP3015")) {
        while (!delayPostBatchMutate) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }

        super.preFlush(e);
        latch.countDown();
      } else {
        super.preFlush(e);
      }
    }
  }

  public static class MoveThread extends Thread {
    private HBaseAdmin admin;
    byte[] regionName;
    byte[] dstRSName;

    public MoveThread(HBaseAdmin admin, byte[] regionName, byte[] dstRSName) {
      this.admin = admin;
      this.regionName = regionName;
      this.dstRSName = dstRSName;
    }

    @Override
    public void run() {
      try {
        this.admin.move(regionName, dstRSName);
      } catch (UnknownRegionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (MasterNotRunningException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ZooKeeperConnectionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

}
