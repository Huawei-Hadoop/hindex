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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.TestUtils;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIndexRegionObserver {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int TTL_SECONDS = 2;
  private static final int TTL_MS = TTL_SECONDS * 1000;
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
    conf.setInt("hbase.hstore.compactionThreshold",5);
    UTIL.startMiniCluster(1);
    admin = new IndexAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (admin != null) admin.close();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    MockIndexRegionObserver.count = 0;
    MockIndexRegionObserver.isnullIndexRegion = false;
  }

  @Test(timeout = 180000)
  public void testPutWithAndWithoutTheIndexedColumn() throws IOException, KeeperException,
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

    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(1, i);

    // Test put without the indexed column
    Put p1 = new Put("row2".getBytes());
    p1.add("col".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p1);

    i = countNumberOfRows(userTableName);
    Assert.assertEquals(2, i);
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

  @Test(timeout = 180000)
  public void testPostOpenCoprocessor() throws IOException, KeeperException, InterruptedException {
    String userTableName = "testPostOpenCoprocessor";
    
    HTableDescriptor ihtd =
      TestUtils.createIndexedHTableDescriptor(userTableName, "col", "Index1", "col", "ql");
    admin.createTable(ihtd);

    // Check the number of indices
    List<IndexSpecification> list = IndexManager.getInstance().getIndicesForTable(userTableName);
    Assert.assertEquals(1, list.size());

    // Check the index name
    boolean bool = false;
    for (IndexSpecification e : list) {
      if (e.getName().equals("Index1")) bool = true;
    }
    Assert.assertTrue(bool);
  }

  @Test(timeout = 180000)
  public void testMultipleIndicesOnUniqueColumns() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testMultipleIndicesOnUniqueColumns";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec1.addIndexColumn(hcd, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd, "ql2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql3".getBytes(), "myValue".getBytes());
    p.add("col".getBytes(), "ql4".getBytes(), "myValue".getBytes());
    table.put(p);
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(0, i);

    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col".getBytes(), "ql2".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(2, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(2, i);
  }

  @Test(timeout = 180000)
  public void testIndexOnMultipleCols() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testSingleIndexOnMultipleCols";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    // Creating and adding the column families
    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    ihtd.addFamily(hcd3);

    // Create and add indices
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec1.addIndexColumn(hcd1, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd2, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd3, "ql1", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    Put p = new Put("row1".getBytes());
    p.add("col1".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col2".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col3".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(2, i);

    p = new Put("row2".getBytes());
    p.add("col1".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col2".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(2, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(4, i);

    p = new Put("row3".getBytes());
    p.add("col1".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col3".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(3, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(6, i);

    p = new Put("row4".getBytes());
    p.add("col2".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    p.add("col3".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(4, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(7, i);
  }

  @Test(timeout = 180000)
  public void testPutsWithPadding() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testPutsWithPadding";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    
    // Creating and adding the column families
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    HColumnDescriptor hcd4 = new HColumnDescriptor("col4");

    ihtd.addFamily(hcd2);
    ihtd.addFamily(hcd3);
    ihtd.addFamily(hcd4);

    // Create and add indices
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec2.addIndexColumn(hcd2, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd3, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd4, "ql1", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    Put p = new Put("row1".getBytes());
    p.add("col2".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(1, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(1, i);

    Result[] result = getTheLastRow(userTableName + Constants.INDEX_TABLE_SUFFIX);
    byte[] rowKey1 = result[0].getRow();

    p = new Put("row2".getBytes());
    p.add("col3".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(2, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(2, i);

    result = getTheLastRow(userTableName + Constants.INDEX_TABLE_SUFFIX);
    byte[] rowKey2 = result[0].getRow();

    Assert.assertEquals(rowKey1.length, rowKey2.length);

    p = new Put("row3".getBytes());
    p.add("col3".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p);
    i = countNumberOfRows(userTableName);
    Assert.assertEquals(3, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(3, i);

    result = getTheLastRow(userTableName + Constants.INDEX_TABLE_SUFFIX);
    byte[] rowKey3 = result[0].getRow();
    Assert.assertEquals(rowKey2.length, rowKey3.length);

    /*
     * p = new Put("row4".getBytes()); p.add("col3".getBytes(), "ql1".getBytes(),
     * "myValuefgacfgn".getBytes()); p.add("col2".getBytes(), "ql1".getBytes(),
     * "myValue".getBytes()); p.add("col4".getBytes(), "ql1".getBytes(), "myValue".getBytes());
     * table.put(p); i = countNumberOfRows(userTableName); Assert.assertEquals(4, i); i =
     * countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX); Assert.assertEquals(4, i);
     * result = getTheLastRow(userTableName + Constants.INDEX_TABLE_SUFFIX); byte[] rowKey4 =
     * result[0].getRow(); Assert.assertEquals(rowKey3.length, rowKey4.length);
     */
  }

  @Test(timeout = 180000)
  public void testBulkPut() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testBulkPut";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    // Creating and adding the column families
    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    ihtd.addFamily(hcd3);

    // Create and add indices
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    IndexSpecification iSpec2 = new IndexSpecification("Index2");
    iSpec1.addIndexColumn(hcd1, "ql1", ValueType.String, 10);
    iSpec2.addIndexColumn(hcd3, "ql1", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    indices.addIndex(iSpec2);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    Thread[] t = new Thread[10];
    for (int i = 0; i < 10; i++) {
      t[i] = new Testthread(conf, userTableName);
    }
    for (int i = 0; i < 10; i++) {
      t[i].start();
    }

    for (int i = 0; i < 10; i++) {
      t[i].join();
    }

    // System.out.println("Woke up");
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(5000, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(10000, i);
    /*
     * HLog log = UTIL.getHBaseCluster().getRegionServer(0).getWAL(); log.getHLogDirectoryName
     * (UTIL.getHBaseCluster().getRegionServer(0).toString()); log.getReader(
     * UTIL.getMiniHBaseCluster().getRegionServer(0).getFileSystem(), path,
     * UTIL.getMiniHBaseCluster().getConfiguration());
     */
  }

  class Testthread extends Thread {
    Configuration conf;
    String userTableName;
    HTable table;

    public Testthread(Configuration conf, String userTableName) throws IOException {
      this.conf = conf;
      this.userTableName = userTableName;
      this.table = new HTable(conf, userTableName);
    }

    public void run() {
      for (int j = 0; j < 500; j++) {
        double d = Math.random();
        byte[] rowKey = Bytes.toBytes(d);
        Put p = new Put(rowKey);
        p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
        p.add(Bytes.toBytes("col2"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
        p.add(Bytes.toBytes("col3"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
        try {
          table.put(p);
        } catch (IOException e) {
        }
      }
    }
  }

  @Test(timeout = 180000)
  public void testBulkPutWithRepeatedRows() throws IOException, KeeperException,
      InterruptedException {
    final Configuration conf = UTIL.getConfiguration();
    final String userTableName = "TestBulkPutWithRepeatedRows";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    // Creating and adding the column families
    HColumnDescriptor hcd1 = new HColumnDescriptor("col1");

    ihtd.addFamily(hcd1);

    // Create and add indices
    IndexSpecification iSpec1 = new IndexSpecification("Index1");

    iSpec1.addIndexColumn(hcd1, "ql1", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    new Thread() {
      @Override
      public void run() {
        try {
          HTable table = new HTable(conf, userTableName);
          List<Put> puts = new ArrayList<Put>(5);
          Put p = new Put("row1".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row2".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row3".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row4".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row5".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          table.put(puts);
        } catch (IOException e) {
        }
      }
    }.start();

    new Thread() {
      @Override
      public void run() {
        try {
          HTable table = new HTable(conf, userTableName);
          List<Put> puts = new ArrayList<Put>(5);
          Put p = new Put("row6".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row7".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row3".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row4".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          p = new Put("row10".getBytes());
          p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql1"), Bytes.toBytes("myValue"));
          puts.add(p);
          table.put(puts);
        } catch (IOException e) {
        }
      }
    }.start();
    Thread.sleep(2000);
    int i = countNumberOfRows(userTableName);
    Assert.assertEquals(8, i);
    i = countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX);
    Assert.assertEquals(8, i);

  }

  @Test(timeout = 180000)
  public void testIndexPutRowkeyWithAllTheValues() throws IOException {
    String DIR = UTIL.getDataTestDir("TestStore").toString();
    Path basedir = new Path(DIR + "TestIndexPut");
    // Path logdir = new Path(DIR+"TestIndexPut"+"/logs");
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestIndexPut"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "A".getBytes(), "B".getBytes(), false);
    HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, htd, null);
    IndexSpecification spec = new IndexSpecification("testSpec");
    spec.addIndexColumn(new HColumnDescriptor("cf1"), "ql1", ValueType.String, 10);
    spec.addIndexColumn(new HColumnDescriptor("cf2"), "ql1", ValueType.String, 10);

    // Scenario where both the indexed cols are there in the put
    byte[] rowKey = "Arow1".getBytes();
    Put p = new Put(rowKey);
    long time = 1234567;
    p.add("cf1".getBytes(), "ql1".getBytes(), time, "testvalue1".getBytes());
    p.add("cf2".getBytes(), "ql1".getBytes(), time + 10, "testvalue1".getBytes());
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    Assert.assertEquals(region.getStartKey().length + 1 + Constants.DEF_MAX_INDEX_NAME_LENGTH + 2
        * 10 + rowKey.length, indexPut.getRow().length);
    Assert.assertEquals(time + 10, indexPut.get(Constants.IDX_COL_FAMILY, "".getBytes()).get(0)
        .getTimestamp());

  }

  @Test(timeout = 180000)
  public void testIndexPutWithOnlyOneValue() throws IOException {
    String DIR = UTIL.getDataTestDir("TestStore").toString();
    Path basedir = new Path(DIR + "TestIndexPut");
    // Path logdir = new Path(DIR+"TestIndexPut"+"/logs");
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("TestIndexPut");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "A".getBytes(), "B".getBytes(), false);
    HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, htd, null);
    IndexSpecification spec = new IndexSpecification("testSpec");
    spec.addIndexColumn(new HColumnDescriptor("cf1"), "ql1", ValueType.String, 10);
    spec.addIndexColumn(new HColumnDescriptor("cf2"), "ql1", ValueType.String, 10);

    byte[] rowKey = "Arow1".getBytes();
    Put p = new Put(rowKey);
    long time = 1234567;
    p.add("cf1".getBytes(), "ql1".getBytes(), time, "testvalue1".getBytes());
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    Assert.assertEquals(region.getStartKey().length + 1 + Constants.DEF_MAX_INDEX_NAME_LENGTH + 2
        * 10 + rowKey.length, indexPut.getRow().length);
    Assert.assertEquals(time, indexPut.get(Constants.IDX_COL_FAMILY, "".getBytes()).get(0)
        .getTimestamp());
    // asserting pad........this has to be hardcoded.
    byte[] pad = new byte[10];
    System.arraycopy(indexPut.getRow(), region.getStartKey().length + 1
        + Constants.DEF_MAX_INDEX_NAME_LENGTH + 10, pad, 0, 10);
    Assert.assertTrue(Bytes.equals(pad, new byte[10]));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithValueGreaterThanLength() throws IOException {
    String DIR = UTIL.getDataTestDir("TestStore").toString();
    Path basedir = new Path(DIR + "TestIndexPut");
    // Path logdir = new Path(DIR+"TestIndexPut"+"/logs");
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("TestIndexPut");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "A".getBytes(), "B".getBytes(), false);
    HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, htd, null);
    IndexSpecification spec = new IndexSpecification("testSpec");
    spec.addIndexColumn(new HColumnDescriptor("cf1"), "ql1", ValueType.String, 10);
    spec.addIndexColumn(new HColumnDescriptor("cf2"), "ql1", ValueType.String, 10);

    // assert IOException when value length goes beyond the limit.
    byte[] rowKey = "Arow1".getBytes();
    Put p = new Put(rowKey);
    long time = 1234567;
    boolean returnVal = false;
    try {
      p.add("cf1".getBytes(), "ql1".getBytes(), time, "testvalue11".getBytes());
      IndexUtils.prepareIndexPut(p, spec, region);
    } catch (IOException e) {
      returnVal = true;
    }
    Assert.assertTrue(returnVal);
  }

  @Test(timeout = 180000)
  public void testIndexPutSequence() throws IOException {
    String DIR = UTIL.getDataTestDir("TestStore").toString();
    Path basedir = new Path(DIR + "TestIndexPut");
    // Path logdir = new Path(DIR+"TestIndexPut"+"/logs");
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestIndexPut"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "A".getBytes(), "B".getBytes(), false);
    HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, htd, null);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("cf1"), "ql1", ValueType.String, 10);
    spec.addIndexColumn(new HColumnDescriptor("cf2"), "ql1", ValueType.String, 10);

    // scenario where same indexName but diff colvalues can disturb the order if
    // used without pad
    byte[] rowKey = "Arow1".getBytes();
    Put p1 = new Put(rowKey);
    long time = 1234567;
    p1.add("cf1".getBytes(), "ql1".getBytes(), time, "testcase".getBytes());
    p1.add("cf2".getBytes(), "ql1".getBytes(), time, "value".getBytes());
    Put indexPut1 = IndexUtils.prepareIndexPut(p1, spec, region);
    Put p2 = new Put(rowKey);
    p2.add("cf1".getBytes(), "ql1".getBytes(), time, "test".getBytes());
    p2.add("cf2".getBytes(), "ql1".getBytes(), time, "value".getBytes());
    Put indexPut2 = IndexUtils.prepareIndexPut(p2, spec, region);
    // (spaces just for easier reading...not present in actual)
    // Index Row key For p1 = "A index testcase value Arow1"
    // Index Row key For p2 = "A index test value Arow1"
    // NOW acc. to the lexographical ordering p2 should come second but we need
    // it to come 1st datz where pad is needed.
    byte[] rowKey1 = indexPut1.getRow();
    byte[] rowKey2 = indexPut2.getRow();

    int result = Bytes.compareTo(rowKey1, rowKey2);
    Assert.assertTrue(result > 0);

    // scenario where the index names are diff and padding is needed.
    IndexSpecification spec1 = new IndexSpecification("ind");
    spec1.addIndexColumn(new HColumnDescriptor("cf3"), "ql1", ValueType.String, 10);
    p1 = new Put(rowKey);
    p1.add("cf3".getBytes(), "ql1".getBytes(), time, "testcase".getBytes());
    indexPut1 = IndexUtils.prepareIndexPut(p1, spec1, region);
    // (spaces just for easier reading...not present in actual)
    // Index Row key For p1 = "A ind testcase value Arow1"
    // Index Row key For p2 = "A index test value Arow1"
    // NOW acc. to the lexographical ordering p1 should come second but we need
    // it to come 1st datz where pad is needed.
    rowKey1 = indexPut1.getRow();
    result = Bytes.compareTo(rowKey1, rowKey2);
    Assert.assertTrue(result < 0);

  }

  @Test(timeout = 180000)
  public void testIndexTableValue() throws IOException {
    String DIR = UTIL.getDataTestDir("TestStore").toString();
    Path basedir = new Path(DIR + "TestIndexPut");
    // Path logdir = new Path(DIR+"TestIndexPut"+"/logs");
    FileSystem fs = UTIL.getTestFileSystem();
    Configuration conf = UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("TestIndexPut");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, htd, null);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("cf1"), "ql1", ValueType.String, 10);
    spec.addIndexColumn(new HColumnDescriptor("cf2"), "ql1", ValueType.String, 10);

    byte[] rowKey = "Arow1".getBytes();
    Put p1 = new Put(rowKey);
    long time = 1234567;
    p1.add("cf1".getBytes(), "ql1".getBytes(), time, "testcase".getBytes());
    p1.add("cf2".getBytes(), "ql1".getBytes(), time, "value".getBytes());
    Put indexPut1 = IndexUtils.prepareIndexPut(p1, spec, region);

    List<Cell> kvs = indexPut1.get(Constants.IDX_COL_FAMILY, "".getBytes());
    Cell kv = null;
    if (null != kvs) {
      kv = kvs.get(0);
    }
    byte[] val = kv.getValue();
    byte[] startKeyLengthInBytes = new byte[2];
    System.arraycopy(val, 0, startKeyLengthInBytes, 0, startKeyLengthInBytes.length);
    int startkeylen = (int) (Bytes.toShort(startKeyLengthInBytes));
    Assert.assertEquals(3, startkeylen);

    byte[] rowKeyOffset = new byte[2];
    System.arraycopy(val, startKeyLengthInBytes.length, rowKeyOffset, 0, rowKeyOffset.length);
    int rowKeyOffsetInt = Bytes.toShort(rowKeyOffset);
    Assert.assertEquals(42, rowKeyOffsetInt);
  }

  @Test(timeout = 180000)
  public void testIndexedRegionAfterSplitShouldReplaceStartKeyAndValue() throws IOException,
      KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIndexRegionSplit";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }

    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();
    Scan s = new Scan();
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      i++;
    }
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    Scan s1 = new Scan();
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    
    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionsOfTable(indexTable);
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());
    int i1 = 0;
    HTable tableidx = new HTable(conf, userTableName + "_idx");
    ResultScanner scanner1 = tableidx.getScanner(s1);
    for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
      i1++;
    }
    Assert.assertEquals("count should be equal", i, i1);
  }

  @Test(timeout = 180000)
  public void
      testIndexedRegionAfterSplitShouldNotThrowExceptionIfThereAreNoSplitFilesForIndexedTable()
          throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIndexRegionSplit1";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    HColumnDescriptor hcd2 = new HColumnDescriptor("col2");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd2);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col2".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }

    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    HTable tableidx = new HTable(conf, userTableName + "_idx");
    Scan s = new Scan();
    int i = 0;
    ResultScanner scanner = table.getScanner(s);
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      i++;
    }
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    Scan s1 = new Scan();
    int i1 = 0;
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
             .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());
    Assert.assertEquals("Main table count shud be 10", 10, i);
    ResultScanner scanner1 = tableidx.getScanner(s1);
    for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
      i1++;
    }
    Assert.assertEquals("Index table count shud be 0", 0, i1);

    // Trying to put data for indexed col
    for (int k = 10; k < 20; k++) {
      String row = "row" + k;
      Put p = new Put(row.getBytes());
      String val = "Val" + k;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    int z = 0;
    Scan s2 = new Scan();
    ResultScanner scanner2 = tableidx.getScanner(s2);

    admin.flush(userTableName + "_idx");

    for (Result rr = scanner2.next(); rr != null; rr = scanner2.next()) {
      z++;
    }
    Assert.assertEquals("Index table count shud be now 10", 10, z);

    List<HRegionInfo> regionsOfTable1 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo1 = regionsOfTable1.get(0);

    admin.split(hRegionInfo1.getRegionName(), "row3".getBytes());
    List<HRegionInfo> mainTableRegions1 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions1 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions1.size() != 3) {
      Thread.sleep(2000);
      mainTableRegions1 =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions1.size() != 3) {
      Thread.sleep(2000);
      indexTableRegions1 =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    // It shud be 3 after one more split
    Assert.assertEquals(3, mainTableRegions1.size());
    Assert.assertEquals(3, indexTableRegions1.size());
    Scan s3 = new Scan();
    int a = 0;
    ResultScanner scanner3 = tableidx.getScanner(s3);
    for (Result rr = scanner3.next(); rr != null; rr = scanner3.next()) {
      a++;
    }
    int b = 0;
    Scan s4 = new Scan();
    ResultScanner scanner4 = table.getScanner(s4);
    for (Result rr = scanner4.next(); rr != null; rr = scanner4.next()) {
      b++;
    }
    // subracting 10 coz addtional 10 is for other col family which is not indexed
    Assert.assertEquals("count should be equal", a, b - 10);

  }

  @Test(timeout = 180000)
  public void testSeekToInIndexHalfStoreFileReaderShouldRetreiveClosestRowCorrectly()
      throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 900000000);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testIndexSplitWithClosestRow";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i <= 10; i++) {
      // Don't put row4 alone so that when getRowOrBefore for row4 is specifiesd row3 shud be
      // returned
      if (i != 4) {
        String row = "row" + i;
        Put p = new Put(row.getBytes());
        String val = "Val" + i;
        p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
        table.put(p);
      }
    }
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();
    HTable tableidx = new HTable(conf, userTableName + "_idx");
    admin.split(hRegionInfo.getRegionName(), "row6".getBytes());
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());
    Scan s1 = new Scan();
    ResultScanner scanner1 = tableidx.getScanner(s1);
    Result next = null;
    for (int i = 0; i < 6; i++) {
      next = scanner1.next();
    }
    String row1 = "row4";
    Put row4Put = new Put(row1.getBytes());
    String val = "Val4";
    row4Put.add("col".getBytes(), "ql".getBytes(), val.getBytes());

    byte[] row4 = row4Put.getRow();
    byte[] nextRow = next.getRow();
    byte[] replacedRow = new byte[nextRow.length];
    System.arraycopy(nextRow, 0, replacedRow, 0, nextRow.length);
    System.arraycopy(row4, 0, replacedRow, replacedRow.length - row4.length, row4.length);
    Result rowOrBefore = tableidx.getRowOrBefore(replacedRow, "d".getBytes());

    String expectedRow = "row3";
    Put p1 = new Put(expectedRow.getBytes());
    String actualStr = Bytes.toString(rowOrBefore.getRow());
    int lastIndexOf = actualStr.lastIndexOf("row3");
    Assert.assertEquals("SeekTo should return row3 as closestRowBefore",
      Bytes.toString(p1.getRow()), actualStr.substring(lastIndexOf, actualStr.length()));
  }

  @Test(timeout = 180000)
  public
      void
      testSeekToInIndexHalfStoreFileReaderShouldRetreiveClosestRowCorrectlyWhenRowIsNotFoundInMainTable()
          throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testIndexSplitWithClosestRowNotInMainTable";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i <= 10; i++) {
      // Don't put row8 alone so that when getRowOrBefore for row8 is specifiesd row7 shud be
      // returned
      if (i != 8) {
        String row = "row" + i;
        Put p = new Put(row.getBytes());
        String val = "Val" + i;
        p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
        table.put(p);
      }
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable tableidx = new HTable(conf, indexTable);
    admin.split(hRegionInfo.getRegionName(), "row4".getBytes());

    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());

    Scan s1 = new Scan();
    ResultScanner scanner1 = tableidx.getScanner(s1);
    Result next1 = null;
    Thread.sleep(3000);
    for (int i = 0; i < 10; i++) {
      next1 = scanner1.next();
    }
    String row1 = "row8";
    Put row4Put = new Put(row1.getBytes());
    String val = "Val8";
    row4Put.add("col".getBytes(), "ql".getBytes(), val.getBytes());
    byte[] row4 = row4Put.getRow();
    byte[] nextRow = next1.getRow();
    byte[] replacedRow = new byte[nextRow.length];
    System.arraycopy(nextRow, 0, replacedRow, 0, nextRow.length);
    System.arraycopy(row4, 0, replacedRow, replacedRow.length - row4.length, row4.length);
    Result rowOrBefore = tableidx.getRowOrBefore(replacedRow, "d".getBytes());

    String expectedRow = "row7";
    Put p1 = new Put(expectedRow.getBytes());
    String actualStr = Bytes.toString(rowOrBefore.getRow());
    int lastIndexOf = actualStr.lastIndexOf("row7");
    Assert.assertTrue("Expected row should have the start key replaced to split key ", Bytes
        .toString(rowOrBefore.getRow()).startsWith("row4"));
    Assert.assertEquals("SeekTo should return row7 as closestRowBefore and split is completed ",
      Bytes.toString(p1.getRow()), actualStr.substring(lastIndexOf, actualStr.length()));
  }

  @Test(timeout = 180000)
  public void testPutWithValueLengthMoreThanMaxValueLength() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testPutWithValueLengthMoreThanMaxValueLength";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec1 = new IndexSpecification("Index1");
    iSpec1.addIndexColumn(hcd, "ql1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    HTable table = new HTable(conf, userTableName);
    table.setAutoFlush(false, false);
    List<Put> putList = new ArrayList<Put>(3);
    putList.add(new Put("row1".getBytes()).add("col".getBytes(), "ql1".getBytes(),
      "valueLengthMoreThanMaxValueLength".getBytes()));
    putList.add(new Put("row2".getBytes()).add("col".getBytes(), "ql1".getBytes(),
      "myValue".getBytes()));
    putList.add(new Put("row3".getBytes()).add("col".getBytes(), "ql1".getBytes(),
      "myValue".getBytes()));
    table.put(putList);
    try {
      table.flushCommits();
    } catch (RetriesExhaustedWithDetailsException e) {
      // nothing to do.
    }
    Assert.assertEquals(1, table.getWriteBuffer().size());
  }

  @Test(timeout = 180000)
  public void testIfPrepareFailsFor2ndSplitShouldFailTheMainTableSplitAlso() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 90000000);
    String userTableName = "testPrepareFailForIdx";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    MockIndexRegionObserver.count++;
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());

    MockIndexRegionObserver.count++;
    List<HRegionInfo> regionsOfTable2 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo1 = regionsOfTable2.get(0);
    admin.split(hRegionInfo1.getRegionName(), "row3".getBytes());
    List<HRegionInfo> mainTableRegions1 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    List<HRegionInfo> indexTableRegions1 =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    Assert.assertEquals(2, mainTableRegions1.size());
    Assert.assertEquals(2, indexTableRegions1.size());
  }

  public static class MockIndexRegionObserver extends IndexRegionObserver {
    static int count = 0;
    static boolean isnullIndexRegion = false;

    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> e,
        byte[] splitKey, List<Mutation> metaEntries) throws IOException {
      if (e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()
          .equals("testIndexManagerWithFailedSplitOfIndexRegion")) {
        throw new IOException();
      }
      if (isnullIndexRegion) {
        e.bypass();
        return;
      }
      if (count == 2) {
        splitThreadLocal.remove();
        e.bypass();
        return;
      } else {
        super.preSplitBeforePONR(e, splitKey,metaEntries);
      }
    }

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
      super.preSplit(e);
    }
  }

  @Test(timeout = 180000)
  public void testShouldNotSplitIfIndexRegionIsNullForIndexTable() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setInt("hbase.regionserver.lease.period", 90000000);
    String userTableName = "testIndexRegionSplit12";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    MockIndexRegionObserver.isnullIndexRegion = true;
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    Assert.assertEquals(1, mainTableRegions.size());
    Assert.assertEquals(1, indexTableRegions.size());
  }

  @Test(timeout = 180000)
  public void testCheckAndPutFor1PutShouldHav2PutsInIndexTableAndShouldReplaceWithNewValue()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCheckAndPutContainingTheIndexedColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    String idxTableName = userTableName + Constants.INDEX_TABLE_SUFFIX;
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "q1".getBytes(), "myValue".getBytes());
    table.put(p);

    int usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    int idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(1, idxtableCount);

    // Test check and put
    Put p1 = new Put("row1".getBytes());
    p1.add("col".getBytes(), "q1".getBytes(), "myNewValue".getBytes());
    Assert.assertTrue(table.checkAndPut("row1".getBytes(), "col".getBytes(), "q1".getBytes(),
      "myValue".getBytes(), p1));
    usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(2, idxtableCount);

    Get get = new Get("row1".getBytes());
    get.addColumn(Bytes.toBytes("col"), Bytes.toBytes("q1"));
    Result result = table.get(get);
    byte[] val = result.getValue(Bytes.toBytes("col"), Bytes.toBytes("q1"));
    Assert.assertEquals("myNewValue", Bytes.toString(val));
  }

  @Test(timeout = 180000)
  public void testCheckAndPutAndNormalPutInParallel() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCheckAndPutAndNormalPutInParallel";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    String idxTableName = userTableName + Constants.INDEX_TABLE_SUFFIX;
    final HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "q1".getBytes(), "myValue".getBytes());
    table.put(p);

    int usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    int idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(1, idxtableCount);

    // Test check and put
    Put p1 = new Put("row1".getBytes());
    p1.add("col".getBytes(), "q1".getBytes(), "myNewValue".getBytes());
    Assert.assertTrue(table.checkAndPut("row1".getBytes(), "col".getBytes(), "q1".getBytes(),
      "myValue".getBytes(), p1));
    new Thread() {
      public void run() {
        Put p = new Put("row1".getBytes());
        p.add("col".getBytes(), "q1".getBytes(), "myValue1".getBytes());
        try {
          table.put(p);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }.start();
    Thread.sleep(3000);
    usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(3, idxtableCount);

  }

  @Test(timeout = 180000)
  public void testCheckAndDeleteShudDeleteTheRowSuccessfullyInBothIndexAndMainTable()
      throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testCheckAndDeleteContainingTheIndexedColumn";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    String idxTableName = userTableName + Constants.INDEX_TABLE_SUFFIX;
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "q1".getBytes(), "myValue".getBytes());
    table.put(p);

    int usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    int idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(1, idxtableCount);

    Delete delete = new Delete("row1".getBytes());
    delete.deleteFamily("col".getBytes());
    // CheckandDelete
    Assert.assertTrue(table.checkAndDelete("row1".getBytes(), "col".getBytes(), "q1".getBytes(),
      "myValue".getBytes(), delete));

    Get get = new Get("row1".getBytes());
    get.addFamily("col".getBytes());
    Result r = table.get(get);
    Assert.assertEquals(0, r.size());

    usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(0, usertableCount);
    idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(0, idxtableCount);
  }

/*  @Test(timeout = 180000)
  public void testRowMutations() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String userTableName = "testMutateRows";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "q1", ValueType.String, 10);
    iSpec.addIndexColumn(hcd, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    String idxTableName = userTableName + Constants.INDEX_TABLE_SUFFIX;
    HTable table = new HTable(conf, userTableName);
    HTable idxtable = new HTable(conf, idxTableName);
    byte[] row = Bytes.toBytes("rowA");

    Put put = new Put(row);
    put.add("col".getBytes(), "q2".getBytes(), "delValue".getBytes());
    table.put(put);

    int usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);
    int idxtableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(1, idxtableCount);

    RowMutations rm = new RowMutations(row);
    Put p = new Put(row);
    p.add("col".getBytes(), "q1".getBytes(), "q1value".getBytes());
    rm.add(p);
    Delete d = new Delete(row);
    d.deleteColumns("col".getBytes(), "q2".getBytes());
    rm.add(d);

    // Mutate rows
    table.mutateRow(rm);

    admin.flush(userTableName);
    admin.majorCompact(userTableName);
    // Now after one put and one delete it shud be 1
    usertableCount = countNumberOfRows(userTableName);
    Assert.assertEquals(1, usertableCount);

    int idxTableCount = countNumberOfRows(idxTableName);
    Assert.assertEquals(1, idxTableCount);

    Get get = new Get(row);
    get.addFamily("col".getBytes());
    Result r = table.get(get);
    Assert.assertEquals(1, r.size());

    String del = Bytes.toString(r.getValue("col".getBytes(), "q2".getBytes()));
    Assert.assertNull(del);
    String putval = Bytes.toString(r.getValue("col".getBytes(), "q1".getBytes()));
    Assert.assertEquals("q1value", putval);

    // Result of index table shud contain one keyvalue for "q1value" put only
    Scan s = new Scan();
    ResultScanner scanner = idxtable.getScanner(s);
    Result result = scanner.next();
    Assert.assertEquals(1, result.size());

    String idxRow = Bytes.toString(result.getRow());
    int len = IndexUtils.getMaxIndexNameLength() + "q1value".length();
    String value = idxRow.substring(IndexUtils.getMaxIndexNameLength() + 1, len + 1);
    // asserting value in idx table is for the put which has the value "q1value"
    Assert.assertEquals("q1value", value);
  }*/

  @Test(timeout = 180000)
  public void testWithPutsAndDeletesAfterSplitShouldRetreiveTheRowsCorrectly() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 900000000);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testPutsAndDeletes";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i <= 5; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
      admin.flush(userTableName);
      admin.flush(userTableName + "_idx");

      if (i < 3) {
        Delete d = new Delete(row.getBytes());
        // Do a normal delete
        table.delete(d);
      } else {
        if (i > 4) {
          Delete d = new Delete(row.getBytes());
          // Do column family delete
          d.deleteFamily("col".getBytes());
          table.delete(d);
        } else {
          Delete d = new Delete(row.getBytes());
          // Do delete column
          d.deleteColumn("col".getBytes(), "ql".getBytes());
          table.delete(d);
        }
      }
      admin.flush(userTableName);
      admin.flush(userTableName + "_idx");
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();
    HTable tableidx = new HTable(conf,indexTable);
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    int mainTableCount = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      mainTableCount++;
    }
    Assert.assertEquals(0, mainTableCount);
    Scan s1 = new Scan();
    ResultScanner scanner1 = tableidx.getScanner(s1);

    int indexTableCount = 0;
    for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
      indexTableCount++;
    }
    Assert.assertEquals(0, indexTableCount);

    Put p = new Put("row7".getBytes());
    String val = "Val" + "7";
    p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
    table.put(p);
    Scan s2 = new Scan();
    ResultScanner scanner2 = table.getScanner(s2);
    for (Result rr = scanner2.next(); rr != null; rr = scanner2.next()) {
      mainTableCount++;
    }
    Scan s3 = new Scan();
    ResultScanner scanner3 = tableidx.getScanner(s3);
    for (Result rr = scanner3.next(); rr != null; rr = scanner3.next()) {
      indexTableCount++;
    }
    Assert.assertEquals(1, mainTableCount);
    Assert.assertEquals(1, indexTableCount);

  }

  @Test(timeout = 180000)
  public void test6PutsAnd3DeletesAfterSplitShouldRetreiveRowsSuccessfully() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 900000000);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "test6Puts3Deletes";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    for (int i = 0; i <= 5; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
      admin.flush(userTableName);
      admin.flush(userTableName + "_idx");
      if (i < 3) {
        Delete d = new Delete(row.getBytes());
        // Do a normal delete
        table.delete(d);
      }
      admin.flush(userTableName);
      admin.flush(userTableName + "_idx");
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();

    HTable tableidx = new HTable(conf, indexTable);
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());
    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> indexTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (indexTableRegions.size() != 2) {
      Thread.sleep(2000);
      indexTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, mainTableRegions.size());
    Assert.assertEquals(2, indexTableRegions.size());
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    int mainTableCount = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      mainTableCount++;
    }
    Assert.assertEquals(3, mainTableCount);
    Scan s1 = new Scan();
    ResultScanner scanner1 = tableidx.getScanner(s1);

    int indexTableCount = 0;
    for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
      indexTableCount++;
    }
    Assert.assertEquals(3, indexTableCount);

  }

  @Test(timeout = 180000)
  public void testSplitForMainTableWithoutIndexShouldBeSuccessfUl() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 900000000);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "test6Puts3Deletes34534";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    ihtd.addFamily(hcd);
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);

    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    List<HRegionInfo> regionsOfTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);
    HRegionInfo hRegionInfo = regionsOfTable.get(0);
    hRegionInfo.getRegionName();
    admin.split(hRegionInfo.getRegionName(), "row5".getBytes());

    List<HRegionInfo> mainTableRegions =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    while (mainTableRegions.size() != 2) {
      Thread.sleep(2000);
      mainTableRegions =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    for (int i = 20; i < 30; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    int mainTableCount = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      mainTableCount++;
    }
    Assert.assertEquals(20, mainTableCount);

  }

  @Test(timeout = 180000)
  public void testSplittingIndexRegionExplicitly() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.lease.period", 900000000);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testSplitTransaction";
    String indexTableName = "testSplitTransaction_idx";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(new HColumnDescriptor("col"), "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    TableName userTable = TableName.valueOf(userTableName);
    TableName indexTable = TableName.valueOf(IndexUtils.getIndexTableName(userTableName));
    HTable table = new HTable(conf, userTableName);

    for (int i = 0; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }

    List<HRegionInfo> regionsOfUserTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(userTable);

    List<HRegionInfo> regionsOfIndexTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);

    // try splitting index.
    admin.split(indexTableName.getBytes());
    Thread.sleep(2000);
    regionsOfIndexTable =
        UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
            .getRegionStates().getRegionsOfTable(indexTable);
    Assert.assertEquals("Index table should not get splited", 1, regionsOfIndexTable.size());

    // try splitting the user region.
    admin.split(userTableName.getBytes(), "row5".getBytes());
    while (regionsOfUserTable.size() != 2) {
      Thread.sleep(2000);
      regionsOfUserTable =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(userTable);
    }
    while (regionsOfIndexTable.size() != 2) {
      Thread.sleep(2000);
      regionsOfIndexTable =
          UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
              .getRegionStates().getRegionsOfTable(indexTable);
    }
    Assert.assertEquals(2, regionsOfUserTable.size());
    Assert.assertEquals(2, regionsOfIndexTable.size());
  }

  @Test(timeout = 180000)
  public void testIndexManagerCleanUp() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testIndexManagerCleanUp";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());

    byte[][] splits = new byte[10][];
    char c = 'A';
    for (int i = 0; i < 10; i++) {
      byte[] b = { (byte) c };
      splits[i] = b;
      c++;
    }
    admin.createTable(ihtd, splits);
    IndexManager instance = IndexManager.getInstance();
    int regionCount = instance.getTableRegionCount(userTableName);
    Assert.assertEquals(11, regionCount);

    admin.disableTable(Bytes.toBytes(userTableName));
    regionCount = instance.getTableRegionCount(userTableName);
    Assert.assertEquals(0, regionCount);

    admin.enableTable(userTableName);
    regionCount = instance.getTableRegionCount(userTableName);
    Assert.assertEquals(11, regionCount);
  }

  @Test(timeout = 180000)
  public void testIndexDataDeletionOnTTLExpiry() throws Exception {
    String userTableName = "testIndexDataDeletionOnTTLExpiry";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));

    HColumnDescriptor hcd1 = new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE);
    IndexSpecification iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd1);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    admin.disableTable(userTableName);
    admin.deleteTable(userTableName);
    while(admin.isTableAvailable(IndexUtils.getIndexTableName(userTableName))){
      Threads.sleep(100);
    }
    ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    hcd1 =
        new HColumnDescriptor("col1").setMaxVersions(Integer.MAX_VALUE).setTimeToLive(
          TTL_SECONDS - 1);
    iSpec = new IndexSpecification("ScanIndexf");
    iSpec.addIndexColumn(hcd1, "q2", ValueType.String, 10);
    ihtd.addFamily(hcd1);
    indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    Configuration conf = UTIL.getConfiguration();
    HTable table = new HTable(conf, userTableName);

    // test put with the indexed column

    Put p = new Put("row1".getBytes());
    p.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p);
    admin.flush(userTableName + "_idx");

    Put p1 = new Put("row01".getBytes());
    p1.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p1);
    admin.flush(userTableName + "_idx");

    Put p2 = new Put("row010".getBytes());
    p2.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p2);
    admin.flush(userTableName + "_idx");

    Put p3 = new Put("row001".getBytes());
    p3.add("col1".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
    table.put(p3);

    admin.flush(userTableName);
    admin.flush(userTableName + "_idx");

    HRegionServer regionServer = UTIL.getHBaseCluster().getRegionServer(0);
    List<HRegion> onlineRegions = regionServer.getOnlineRegions(TableName.valueOf(userTableName));
    byte[][] columns = new byte[1][];
    columns[0] = hcd1.getName();
    byte[][] indexColumns = new byte[1][];
    indexColumns[0] = Constants.IDX_COL_FAMILY;
    
    List<String> storeFileList =
        onlineRegions.get(0).getStoreFileList(columns);
    onlineRegions =
        regionServer
            .getOnlineRegions(TableName.valueOf(IndexUtils.getIndexTableName(userTableName)));
    storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    while (storeFileList.size() < 4) {
      Thread.sleep(1000);
      storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    }
    int prevSize = storeFileList.size();
    Assert.assertEquals("The total store files for the index table should be 4", 4, prevSize);
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
    storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);;
    while (storeFileList.size() != 1) {
      Thread.sleep(1000);
      storeFileList = onlineRegions.get(0).getStoreFileList(indexColumns);
    }
    Assert.assertEquals("The total store files for the index table should be 1", 1,
      storeFileList.size());
    s = new Scan();
    indexTable = new HTable(conf, userTableName + "_idx");
    scanner = indexTable.getScanner(s);
    // Result res = scanner.next();
    boolean dataAvailable = false;
    for (Result result : scanner) {
      dataAvailable = true;
      System.out.println(result);
    }
    Assert.assertFalse("dataShould not be retrieved", dataAvailable);
  }

  @Test(timeout = 180000)
  public void testIndexManagerWithSplitTransactions() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testIndexManagerWithSplitTransactions";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    IndexManager manager = IndexManager.getInstance();
    int count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(1, count);

    HTable table = new HTable(conf, userTableName);
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql"), Bytes.toBytes("test_val"));
      table.put(p);
    }

    admin.split(userTableName, "row5");
    Threads.sleep(10000);
    ZKAssign.blockUntilNoRIT(zkw);
    UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(userTableName));
    count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(2, count);
  }

  @Test(timeout = 180000)
  public void testIndexManagerWithFailedSplitOfIndexRegion() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testIndexManagerWithFailedSplitOfIndexRegion";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    IndexManager manager = IndexManager.getInstance();
    int count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(1, count);

    HTable table = new HTable(conf, userTableName);
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql"), Bytes.toBytes("test_val"));
      table.put(p);
    }

    admin.split(userTableName);

    count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(1, count);
  }

  @Test(timeout = 180000)
  public void testIndexManagerWithFailedSplitTransaction() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean("hbase.use.secondary.index", true);
    String userTableName = "testIndexManagerWithFailedSplitTransaction";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    ihtd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);

    IndexManager manager = IndexManager.getInstance();
    int count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(1, count);

    HTable table = new HTable(conf, userTableName);
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("col1"), Bytes.toBytes("ql"), Bytes.toBytes("test_val"));
      table.put(p);
    }
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(Bytes.toBytes(userTableName));
    HRegionServer rs = UTIL.getMiniHBaseCluster().getRegionServer(0);
    SplitTransaction st = null;

    st = new MockedSplitTransaction(regions.get(0), "row5".getBytes());
    try {
      st.prepare();
      st.execute(rs, rs);
    } catch (IOException e) {
      st.rollback(rs, rs);
    }

    count = manager.getTableRegionCount(userTableName);
    Assert.assertEquals(1, count);
  }

  public static class MockedSplitTransaction extends SplitTransaction {

    public MockedSplitTransaction(HRegion r, byte[] splitrow) {
      super(r, splitrow);
    }
    
    public void splitStoreFiles(Map<byte[], List<StoreFile>> hstoreFilesToSplit) throws IOException {
      throw new IOException();
    }
    
  }
}