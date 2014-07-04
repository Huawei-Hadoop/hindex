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
package org.apache.hadoop.hbase.index.io;

import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIndexHalfStoreFileReaderWithEncoding {
  private static final String COL1 = "c1";
  private static final String CF1 = "cf1";
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.set("index.data.block.encoding.algo", "PREFIX");
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testWithFastDiffEncoding() throws Exception {
    HBaseAdmin admin = new IndexAdmin(UTIL.getConfiguration());
    String tableName = "testWithFastDiffEncoding";
    String idxTableName = "testWithFastDiffEncoding_idx";
    HTableDescriptor htd = TestUtils.createIndexedHTableDescriptor(tableName, CF1, "idx1", CF1, COL1);
    admin.createTable(htd);
    HTable ht = new HTable(UTIL.getConfiguration(), tableName);
    HTable hti = new HTable(UTIL.getConfiguration(), idxTableName);
    Put put = new Put("a".getBytes());
    put.add(CF1.getBytes(), COL1.getBytes(), "1".getBytes());
    ht.put(put);
    put = new Put("d".getBytes());
    put.add(CF1.getBytes(), COL1.getBytes(), "1".getBytes());
    ht.put(put);
    put = new Put("k".getBytes());
    put.add(CF1.getBytes(), COL1.getBytes(), "1".getBytes());
    ht.put(put);
    put = new Put("z".getBytes());
    put.add(CF1.getBytes(), COL1.getBytes(), "1".getBytes());
    ht.put(put);
    Delete delete = new Delete("z".getBytes());
    ht.delete(delete);
    admin.flush(tableName);
    admin.flush(idxTableName);
    NavigableMap<HRegionInfo, ServerName> regionLocations = ht.getRegionLocations();
    byte[] regionName = null;
    for (Entry<HRegionInfo, ServerName> e : regionLocations.entrySet()) {
      regionName = e.getKey().getRegionName();
      break;
    }
    // Splitting the single region.
    admin.split(regionName, "e".getBytes());
    // Sleeping so that the compaction can complete.
    // Split will initiate a compaction.
    Thread.sleep(5 * 1000);
    Scan scan = new Scan();
    ResultScanner scanner = hti.getScanner(scan);
    Result res = scanner.next();
    int count = 0;
    while (res != null) {
      count++;
      res = scanner.next();
    }
    assertEquals(3, count);
    admin.close();
  }

}
