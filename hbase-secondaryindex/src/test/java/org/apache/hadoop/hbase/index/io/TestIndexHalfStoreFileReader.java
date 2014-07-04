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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestIndexHalfStoreFileReader {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  private KeyValue seekToKeyVal;
  private byte[] expectedRow;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
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

  @Test
  public void testIndexHalfStoreFileReaderWithSeekTo() throws Exception {
    HBaseTestingUtility test_util = new HBaseTestingUtility();
    String root_dir = test_util.getDataTestDir("TestIndexHalfStoreFile").toString();
    Path p = new Path(root_dir, "test");
    Configuration conf = test_util.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(1024).build();

    HFile.Writer w =
        HFile.getWriterFactory(conf, cacheConf).withPath(fs, p).withFileContext(meta)
            .withComparator(KeyValue.COMPARATOR).create();
    String usertableName = "testIndexHalfStore";
    List<KeyValue> items = genSomeKeys(usertableName);
    for (KeyValue kv : items) {
      w.append(kv);
    }
    w.close();
    HFile.Reader r = HFile.createReader(fs, p, cacheConf, conf);
    r.loadFileInfo();
    byte[] midkey = "005".getBytes();
    Reference top = new Reference(midkey, Reference.Range.top);
    doTestOfScanAndReseek(p, fs, top, cacheConf, conf);
    r.close();
  }

  private void doTestOfScanAndReseek(Path p, FileSystem fs, Reference bottom,
      CacheConfig cacheConf, Configuration conf) throws IOException {
    final IndexHalfStoreFileReader halfreader =
        new IndexHalfStoreFileReader(fs, p, cacheConf, bottom, conf);
    halfreader.loadFileInfo();
    final HFileScanner scanner = halfreader.getScanner(false, false);
    KeyValue getseekTorowKey3 = getSeekToRowKey();
    scanner.seekTo(getseekTorowKey3.getBuffer(), 8, 17);
    boolean next = scanner.next();
    KeyValue keyValue = null;
    if (next) {
      keyValue = scanner.getKeyValue();
    }
    byte[] expectedRow = getExpected();
    byte[] actualRow = keyValue.getRow();
    Assert.assertArrayEquals(expectedRow, actualRow);
    halfreader.close(true);
  }

  private List<KeyValue> genSomeKeys(String userTableName) throws Exception {
    List<KeyValue> ret = new ArrayList<KeyValue>(4);
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd1 = new HColumnDescriptor("column1");
    HColumnDescriptor hcd2 = new HColumnDescriptor("column2");
    IndexSpecification iSpec1 = new IndexSpecification("Index");
    iSpec1.addIndexColumn(hcd1, "q", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd2, "q", ValueType.String, 10);
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd2);
    TableIndices indices = new TableIndices();
    indices.addIndex(iSpec1);
    ihtd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    admin.createTable(ihtd);
    ByteArrayBuilder indexColVal = ByteArrayBuilder.allocate(4);
    indexColVal.put(Bytes.toBytes((short) 3));
    indexColVal.put(Bytes.toBytes((short) 32));

    Put p1 = generatePuts("006".getBytes(), "05".getBytes());
    Put p2 = generatePuts("003".getBytes(), "06".getBytes());
    Put p3 = generatePuts("004".getBytes(), "06".getBytes());
    Put p4 = generatePuts("007".getBytes(), "06".getBytes());

    byte[] seekToPut =
        new byte[3 + 1 + IndexUtils.getMaxIndexNameLength() + 10 + "006".getBytes().length];
    System.arraycopy(p1.getRow(), 0, seekToPut, 0, p1.getRow().length);
    byte[] seekToRow = "007".getBytes();
    System.arraycopy(seekToRow, 0, seekToPut, p1.getRow().length - 3, seekToRow.length);
    System.arraycopy("005".getBytes(), 0, seekToPut, 0, 3);
    setSeekToRowKey(seekToPut, indexColVal);

    byte[] expectedPut =
        new byte[3 + 1 + IndexUtils.getMaxIndexNameLength() + 10 + "006".getBytes().length];
    System.arraycopy(p4.getRow(), 0, expectedPut, 0, p4.getRow().length);
    // Copy first 3 bytes to splitKey since getKeyValue will replace the start key with splitKey.
    // Just for assertion this is been added
    System.arraycopy("005".getBytes(), 0, expectedPut, 0, 3);
    setExpected(expectedPut);

    KeyValue kv =
        new KeyValue(p1.getRow(), Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0,
            indexColVal.array());
    ret.add(kv);
    KeyValue kv1 =
        new KeyValue(p2.getRow(), Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0,
            indexColVal.array());
    ret.add(kv1);
    KeyValue kv2 =
        new KeyValue(p3.getRow(), Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0,
            indexColVal.array());
    ret.add(kv2);
    KeyValue kv3 =
        new KeyValue(p4.getRow(), Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0,
            indexColVal.array());
    ret.add(kv3);
    return ret;
  }

  private void setSeekToRowKey(byte[] seekTorowKey3, ByteArrayBuilder indexColVal) {
    KeyValue kv =
        new KeyValue(seekTorowKey3, Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0,
            indexColVal.array());
    this.seekToKeyVal = kv;
  }

  private KeyValue getSeekToRowKey() {
    return this.seekToKeyVal;
  }

  private void setExpected(byte[] expected) {
    this.expectedRow = expected;
  }

  private byte[] getExpected() {
    return this.expectedRow;
  }

  private Put generatePuts(byte[] rowKey, byte[] val) throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String usertableName = "testIndexHalfStore";
    HTable table = new HTable(conf, usertableName + "_idx");
    byte[] onebyte = new byte[1];
    String indexName = "Indexname";
    byte[] remIndex = new byte[IndexUtils.getMaxIndexNameLength() - indexName.length()];
    byte[] valPad = new byte[8];
    ByteArrayBuilder indexColVal = ByteArrayBuilder.allocate(4);
    indexColVal.put(Bytes.toBytes((short) 3));
    indexColVal.put(Bytes.toBytes((short) 32));
    byte[] put = new byte[3 + 1 + IndexUtils.getMaxIndexNameLength() + 10 + rowKey.length];
    System.arraycopy("000".getBytes(), 0, put, 0, 3);
    System.arraycopy(onebyte, 0, put, 3, onebyte.length);
    System.arraycopy(indexName.getBytes(), 0, put, 3 + onebyte.length, indexName.length());
    System.arraycopy(remIndex, 0, put, 3 + onebyte.length + indexName.length(), remIndex.length);
    System.arraycopy(val, 0, put, 3 + onebyte.length + indexName.length() + remIndex.length,
      val.length);
    System.arraycopy(valPad, 0, put, 3 + onebyte.length + indexName.length() + remIndex.length
        + val.length, valPad.length);
    System.arraycopy(rowKey, 0, put, 3 + onebyte.length + indexName.length() + remIndex.length
        + val.length + valPad.length, rowKey.length);
    Put p = new Put(put);
    p.add(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, 0, indexColVal.array());
    table.put(p);
    return p;
  }
}
