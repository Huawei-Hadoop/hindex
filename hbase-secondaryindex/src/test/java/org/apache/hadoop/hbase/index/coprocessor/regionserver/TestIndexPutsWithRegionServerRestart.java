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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.TestIndexRegionObserver.MockIndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIndexPutsWithRegionServerRestart {
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, MockIndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
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

  @Test(timeout = 180000)
  public void testShouldRetrieveIndexPutsOnRSRestart() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testPutContainingTheIndexedColumn";
    HTableDescriptor ihtd =
        TestUtils.createIndexedHTableDescriptor(userTableName, "col", "Index1", "col", "ql");
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    table.put(p);

    // Thread.sleep(2000);
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(1, i);

    HRegionServer regionServer = UTIL.getHBaseCluster().getRegionServer(0);
    HMaster master = UTIL.getHBaseCluster().getMaster();
    regionServer.abort("Aborting region server");
    while (master.getServerManager().areDeadServersInProgress()) {
      Thread.sleep(1000);
    }
    UTIL.getHBaseCluster().startRegionServer();
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(1, i);
  }

  public int countNumberOfRows(String tableName) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, tableName);
    Scan s = new Scan();
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      i++;
      result = scanner.next(1);
    }
    return i;
  }

  public Result[] getTheLastRow(String tableName) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, tableName);
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result[] result = scanner.next(1);
    Result[] result1 = result;
    while (result1 != null && result1.length > 0) {
      result1 = scanner.next(1);
      if (null == result1 || result1.length <= 0) break;
      else result = result1;
    }
    return result;
  }

}
