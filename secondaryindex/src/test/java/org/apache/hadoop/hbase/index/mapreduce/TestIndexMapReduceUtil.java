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
package org.apache.hadoop.hbase.index.mapreduce;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIndexMapReduceUtil {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;
  private Configuration conf;
  private String tableName;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    admin = UTIL.getHBaseAdmin();
    conf = UTIL.getConfiguration();
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test(timeout = 180000)
  public void testShouldAbleReturnTrueForIndexedTable() throws Exception {
    tableName = "testShouldAbleReturnTrueForIndexedTable";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec);
    admin.createTable(ihtd);
    Assert.assertTrue(IndexMapReduceUtil.isIndexedTable(tableName, conf));
  }

  @Test(timeout = 180000)
  public void testShouldAbleReturnFalseForNonIndexedTable() throws Exception {
    tableName = "testShouldAbleReturnFalseForNonIndexedTable";
    HTableDescriptor ihtd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    ihtd.addFamily(hcd);
    admin.createTable(ihtd);
    Assert.assertFalse(IndexMapReduceUtil.isIndexedTable(tableName, conf));
  }

  @Test(timeout = 180000)
  public void testShouldReturnStartKeyBesedOnTheRowKeyFromPreSplitRegion() throws Exception {

    tableName = "testShouldReturnStartKeyBesedOnTheRowKeyFromPreSplitRegion";
    HTable table =
        UTIL.createTable(tableName.getBytes(), new byte[][] { "families".getBytes() }, 3,
          "0".getBytes(), "9".getBytes(), 5);

    assertStartKey(conf, tableName, table, "3");
    assertStartKey(conf, tableName, table, "0");
    assertStartKey(conf, tableName, table, "25");
    assertStartKey(conf, tableName, table, "AAAAA123");
    assertStartKey(conf, tableName, table, "63");
    assertStartKey(conf, tableName, table, "");
    assertStartKey(conf, tableName, table, "9222");
  }

  @Test(timeout = 180000)
  public void testShouldReturnStartKeyBesedOnTheRowKey() throws Exception {

    tableName = "testShouldReturnStartKeyBesedOnTheRowKey";
    HTable table = UTIL.createTable(tableName.getBytes(), new byte[][] { "families".getBytes() });

    assertStartKey(conf, tableName, table, "3");
    assertStartKey(conf, tableName, table, "0");
    assertStartKey(conf, tableName, table, "25");
    assertStartKey(conf, tableName, table, "AAAAA123");
    assertStartKey(conf, tableName, table, "");
  }

  @Test(timeout = 180000)
  public void testShouldFormIndexPutsAndIndexDeletes() throws Exception {
    tableName = "testShouldFormIndexPutsAndIndexDeletes";
    IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addIndex(iSpec);
    admin.getConfiguration().set(TableInputFormat.INPUT_TABLE, tableName);
    admin.createTable(ihtd);
    HTable mainTable = new HTable(conf, Bytes.toBytes(tableName));
    Put put = new Put(Bytes.toBytes("r1"));
    put.add(hcd.getName(), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    mainTable.put(put);
    put = new Put(Bytes.toBytes("r2"));
    put.add(hcd.getName(), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    mainTable.put(put);
    put = new Put(Bytes.toBytes("r3"));
    put.add(hcd.getName(), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    put.add(hcd.getName(), Bytes.toBytes("q2"), Bytes.toBytes("v2"));
    mainTable.put(put);
    put = new Put(Bytes.toBytes("r4"));
    put.add(hcd.getName(), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    mainTable.put(put);
    put = new Put(Bytes.toBytes("r5"));
    put.add(hcd.getName(), Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    mainTable.put(put);
    mainTable.flushCommits();
    admin.flush(tableName);
    Delete del = new Delete(Bytes.toBytes("r3"));
    del.deleteFamily(hcd.getName());
    mainTable.delete(del);
    List<Delete> indexDelete = IndexMapReduceUtil.getIndexDelete(del, admin.getConfiguration());
    assertTrue(indexDelete.size() == 0);
    admin.flush(tableName);
    del = new Delete(Bytes.toBytes("r5"));
    del.deleteColumns(hcd.getName(), Bytes.toBytes("q1"));
    mainTable.delete(del);
    indexDelete = IndexMapReduceUtil.getIndexDelete(del, admin.getConfiguration());
    Map<byte[], List<KeyValue>> familyMap = ((Delete) indexDelete.get(0)).getFamilyMap();
    Set<Entry<byte[], List<KeyValue>>> entrySet = familyMap.entrySet();
    for (Entry<byte[], List<KeyValue>> entry : entrySet) {
      List<KeyValue> value = entry.getValue();
      assertTrue(!value.get(0).isDeleteFamily());
    }

  }

  private void assertStartKey(Configuration conf, String tableName, HTable table, String rowKey)
      throws IOException {
    byte[] startKey = IndexMapReduceUtil.getStartKey(conf, tableName, Bytes.toBytes(rowKey));
    Assert.assertEquals("Fetching wrong start key for " + rowKey,
      Bytes.toString(table.getRegionLocation(rowKey).getRegionInfo().getStartKey()),
      Bytes.toString(startKey));
  }
}
