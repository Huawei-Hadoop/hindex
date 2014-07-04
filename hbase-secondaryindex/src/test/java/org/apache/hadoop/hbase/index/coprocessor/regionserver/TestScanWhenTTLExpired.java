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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestScanWhenTTLExpired {

  private static HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;
  private static final int NB_SERVERS = 1;
  private static final int TTL_SECONDS = 2;
  private static final int TTL_MS = TTL_SECONDS * 1000;

  private static final HBaseTestingUtility TESTING_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void before() throws Exception {
    Configuration conf = TESTING_UTIL.getConfiguration();
    conf.setInt("hbase.balancer.period", 60000);
    // Needed because some tests have splits happening on RS that are killed
    // We don't want to wait 3min for the master to figure it out
    conf.setInt("hbase.master.assignment.timeoutmonitor.timeout", 4000);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setInt("hbase.hstore.compactionThreshold",5);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
    
  }

  @Before
  public void setup() throws IOException {
    TESTING_UTIL.ensureSomeRegionServersAvailable(NB_SERVERS);
    this.admin = new IndexAdmin(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  @AfterClass
  public static void after() throws Exception {
    if (admin != null) admin.close();
    TESTING_UTIL.shutdownMiniCluster();
  }


  @Test(timeout = 180000)
  public void testScannerSelectionWhenPutHasOneColumn() throws IOException, KeeperException,
      InterruptedException {
    String userTableName = "testScannerSelectionWhenPutHasOneColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd =
        new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(TTL_SECONDS);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    Configuration conf = TESTING_UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    table.put(p);

    Put p1 = new Put("row01".getBytes());
    p1.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    table.put(p1);

    Put p2 = new Put("row010".getBytes());
    p2.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    table.put(p2);

    Put p3 = new Put("row001".getBytes());
    p3.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    table.put(p3);

    admin.flush(userTableName);

    HRegionServer regionServer = TESTING_UTIL.getHBaseCluster().getRegionServer(0);
    List<HRegion> onlineRegions = regionServer.getOnlineRegions(TableName.valueOf(userTableName));
    byte[][] columns = new byte[1][];
    columns[0] = hcd.getName();
    List<String> storeFileList =
        onlineRegions.get(0).getStoreFileList(columns);

    for (String store : storeFileList) {
      Threads.sleepWithoutInterrupt(TTL_MS);
    }
    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    assertEquals("No rows should be retrieved", 0, i);
  }

  @Test(timeout = 180000)
  public void testScannerSelectionWhenThereAreMutlipleCFs() throws IOException, KeeperException,
      InterruptedException {
    String userTableName = "testScannerSelectionWhenThereAreMutlipleCFs";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd =
        new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(
          Integer.MAX_VALUE);
    HColumnDescriptor hcd1 =
        new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(
          TTL_SECONDS - 1);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    Configuration conf = TESTING_UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column

    Put p = new Put("row1".getBytes());
    p.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p);

    Put p1 = new Put("row01".getBytes());
    p1.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p1.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p1);

    Put p2 = new Put("row010".getBytes());
    p2.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p2.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p2);

    Put p3 = new Put("row001".getBytes());
    p3.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p3.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p3);

    admin.flush(userTableName);

    HRegionServer regionServer = TESTING_UTIL.getHBaseCluster().getRegionServer(0);
    List<HRegion> onlineRegions = regionServer.getOnlineRegions(TableName.valueOf(userTableName));
    byte[][] columns = new byte[1][];
    columns[0] = hcd.getName();
    List<String> storeFileList =
        onlineRegions.get(0).getStoreFileList(columns);

    for (String store : storeFileList) {
      Threads.sleepWithoutInterrupt(TTL_MS);
    }
    int i = countNumberOfRowsWithFilter(userTableName, "Val", true, false, 0);
    assertEquals("No rows should be retrieved", 0, i);

  }

  @Test(timeout = 180000)
  public void testCompactionOnIndexTableShouldNotRetrieveTTLExpiredData() throws Exception {
    String userTableName = "testCompactionOnIndexTableShouldNotRetrieveTTLExpiredData";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd =
        new HColumnDescriptor("col").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(
          TTL_SECONDS - 1);
    HColumnDescriptor hcd1 =
        new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(
          TTL_SECONDS - 1);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    Configuration conf = TESTING_UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column

    Put p = new Put("row1".getBytes());
    p.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p);
    admin.flush(userTableName + "_idx");

    Put p1 = new Put("row01".getBytes());
    p1.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p1.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p1);
    admin.flush(userTableName + "_idx");

    Put p2 = new Put("row010".getBytes());
    p2.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p2.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p2);
    admin.flush(userTableName + "_idx");

    Put p3 = new Put("row001".getBytes());
    p3.add(Bytes.toBytes("col"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
    p3.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p3);

    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");

    HRegionServer regionServer = TESTING_UTIL.getHBaseCluster().getRegionServer(0);
    List<HRegion> onlineRegions = regionServer.getOnlineRegions(TableName.valueOf(userTableName));
    byte[][] columns = new byte[1][];
    columns[0] = hcd.getName();
    List<String> storeFileList =
        onlineRegions.get(0).getStoreFileList(columns);
    onlineRegions =
        regionServer
            .getOnlineRegions(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));
    byte[][] indexColumns = new byte[1][];
    indexColumns[0] = Constants.IDX_COL_FAMILY;
    storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    while (storeFileList.size() < 4) {
      Thread.sleep(1000);
      storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    }
    int prevSize = storeFileList.size();
    assertEquals("The total store files for the index table should be 4", 4, prevSize);
    Scan s = new Scan();
    HTable indexTable = new HTable(conf, userTableName + "_idx");
    ResultScanner scanner = indexTable.getScanner(s);
    // Result res = scanner.next();
    for (Result result : scanner) {
      System.out.println(result);
    }
    for (String store : storeFileList) {
      Threads.sleepWithoutInterrupt(TTL_MS);
    }
    admin.majorCompact(userTableName + "_idx");

    onlineRegions =
        regionServer
            .getOnlineRegions(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));
    storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    while (storeFileList.size() != 1) {
      Thread.sleep(1000);
      storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    }
    assertEquals("The total store files for the index table should be 1", 1, storeFileList.size());
    s = new Scan();
    indexTable = new HTable(conf, userTableName + "_idx");
    scanner = indexTable.getScanner(s);
    // Result res = scanner.next();
    boolean dataAvailable = false;
    for (Result result : scanner) {
      dataAvailable = true;
      System.out.println(result);
    }
    assertFalse("dataShould not be retrieved", dataAvailable);

  }

  private int countNumberOfRowsWithFilter(String tableName, String filterVal, boolean isIndexed,
      boolean isCached, int cacheNumber) throws IOException {
    Configuration conf = TESTING_UTIL.getConfiguration();
    HTable table = new HTable(conf, tableName);
    Scan s = new Scan();
    Filter filter = null;
    if (isIndexed) {
      filter =
          new SingleColumnValueFilter(Bytes.toBytes("col"), Bytes.toBytes("q1"), CompareOp.EQUAL,
              filterVal.getBytes());
    } else {
      filter =
          new SingleColumnValueFilter(Bytes.toBytes("col"), Bytes.toBytes("q1"), CompareOp.EQUAL,
              "cat".getBytes());
    }
    s.setFilter(filter);
    if (isCached) {
      s.setCaching(cacheNumber);
    }
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result result : scanner) {
      i++;
    }
    return i;
  }
}
