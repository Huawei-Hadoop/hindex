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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.mapreduce.TableIndexer;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestForComplexIssues {
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  public static CountDownLatch latch = new CountDownLatch(1);
  public static CountDownLatch latchForCompact = new CountDownLatch(1);
  private static boolean compactionCalled = false;
  private static volatile boolean closeCalled = false;
  private static volatile boolean preCloseCalled = false;
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
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
  }

  @Before
  public void setUp() throws Exception {
    UTIL.startMiniCluster(2);
    admin = new IndexAdmin(UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    compactionCalled = false;
    closeCalled = false;
    preCloseCalled = false;
    delayPostBatchMutate = false;
    if(admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }
  
  /**
   * Test verifies whether puts will fail when user region is online but index region is closed.
   * This scenario may come when moving user and index regions to other RS and compaction is
   * progress for user region.
   * @throws Exception
   */
  @Test(timeout = 180000)
  public void testPutsShouldFailWhenIndexRegionIsClosedButUserRegionOnline()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.client.retries.number", 1);
    String userTableName = "testPutsShouldFailWhenIndexRegionIsClosedButUserRegionOnline";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "index_name", "cf", "ql");
    admin.createTable(ihtd);
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

    while (!preCloseCalled) {
      Thread.sleep(200);
    }
    regions = hBaseCluster.getRegions(Bytes.toBytes(userTableName + "_idx"));
    HRegion hRegion1 = regions.get(0);
    encodedNameAsBytes = hRegion1.getRegionInfo().getEncodedNameAsBytes();
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
    List<HRegion> onlineRegions = destServer.getOnlineRegions(TableName.valueOf(userTableName));
    List<HRegion> onlineIdxRegions =
        destServer.getOnlineRegions(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));
    while (onlineRegions.size() == 0 || onlineIdxRegions.size() == 0) {
      Thread.sleep(1000);
      onlineRegions = destServer.getOnlineRegions(TableName.valueOf(userTableName));
      onlineIdxRegions =
          destServer
              .getOnlineRegions(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));
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
  public void testWalReplayShouldNotSkipAnyRecords() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    final String userTableName = "testWalReplayShouldNotSkipAnyRecords";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "index_name", "col", "ql");
    admin.createTable(ihtd);
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
    byte[][] indexColumns = new byte[1][];
    indexColumns[0] = Constants.IDX_COL_FAMILY;
    List<String> storeFileList = indexHRegion.getStoreFileList(indexColumns);
    while (storeFileList.size() == 0) {
      storeFileList = indexHRegion.getStoreFileList(indexColumns);
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
            .getRegionStates().isRegionOnline(indexHRegion.getRegionInfo());
    while (!regionOnline) {
      regionOnline =
          hBaseCluster.getMaster().getAssignmentManager()
              .getRegionStates().isRegionOnline(indexHRegion.getRegionInfo());
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
        long earliestPutTs, InternalScanner s, CompactionRequest request) throws IOException {
      if (store.getTableName().getNameAsString()
          .equals("testPutsShouldFailWhenIndexRegionIsClosedButUserRegionOnline")) {
        try {
          compactionCalled = true;
          latchForCompact.await();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s, request);
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
      if (e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()
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
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested)
        throws IOException {
      if (c.getEnvironment().getRegion().getTableDesc().getNameAsString()
          .equals("testPutsShouldFailWhenIndexRegionIsClosedButUserRegionOnline")
          && closeCount == 0) {
        preCloseCalled = true;
      }
      super.preClose(c, abortRequested);
    }
    
    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
      if (e.getEnvironment().getRegion().getTableDesc().getNameAsString()
          .equals("testPutsShouldFailWhenIndexRegionIsClosedButUserRegionOnline_idx")
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
        MiniBatchOperationInProgress<Mutation> miniBatchOp) {
      if (ctx.getEnvironment().getRegion().getTableDesc().getNameAsString()
          .contains("testWalReplayShouldNotSkipAnyRecords")) {
        if (delayPostBatchMutate) {
          try {
            latch.await();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        super.postBatchMutate(ctx, miniBatchOp);
      } else {
        super.postBatchMutate(ctx, miniBatchOp);
      }
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
      if (e.getEnvironment().getRegion().getTableDesc().getNameAsString()
          .contains("testWalReplayShouldNotSkipAnyRecords")) {
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
      } catch (HBaseIOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

}
