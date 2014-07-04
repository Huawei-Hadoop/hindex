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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SeparatorPartition;
import org.apache.hadoop.hbase.index.SpatialPartition;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestExtendedPutOps {
  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestHRegion").toString();

  @Test(timeout = 180000)
  public void testPutWithOneUnitLengthSeparator() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testPutWithOneUnitLengthSeparator"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("_", 4),
      ValueType.String, 10);

    byte[] value1 = "2ndFloor_solitaire_huawei_bangalore_karnataka".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[10];
    System.arraycopy("bangalore".getBytes(), 0, expectedResult, 0, "bangalore".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "2ndFloor_solitaire_huawei_bangal".getBytes();
    p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    System.arraycopy("bangal".getBytes(), 0, expectedResult, 0, "bangal".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "2ndFloor_solitaire_huawei_".getBytes();
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testPutWithOneAsSplit() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testPutWithOneUnitLengthSeparator"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("---", 1),
      ValueType.String, 10);

    byte[] value1 = "AB---CD---EF---GH---IJ---KL---MN---OP---".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[10];
    System.arraycopy("AB".getBytes(), 0, expectedResult, 0, "AB".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "---CD---EF---GH---IJ---KL---MN---OP---".getBytes();
    p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB".getBytes();
    p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    System.arraycopy("AB".getBytes(), 0, expectedResult, 0, "AB".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "".getBytes();
    p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testPutWithOneUnitLengthSeparatorWithoutValue() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testPutWithOneUnitLengthSeparatorWithoutValue"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("_", 4),
      ValueType.String, 10);
    byte[] value1 = "2ndFloor_solitaire_huawei__karnataka".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[10];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithMultipleUnitLengthSeparator() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithMultipleUnitLengthSeparator"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("---", 6),
      ValueType.String, 10);

    byte[] value1 = "AB---CD---EF---GH---IJ---KL---MN---OP---".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[10];
    System.arraycopy("KL".getBytes(), 0, expectedResult, 0, "KL".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB---CD---EF---GH---IJ---K".getBytes();
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    System.arraycopy("K".getBytes(), 0, expectedResult, 0, "K".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB---CD---EF---GH---".getBytes();
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithMultipleUnitLengthWithSimilarStringPattern() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithMultipleUnitLengthSeparator"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("---", 6),
      ValueType.String, 10);

    byte[] value1 = "AB---CD---EF---GH---IJ---K-L---MN---OP---".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[10];
    System.arraycopy("K-L".getBytes(), 0, expectedResult, 0, "K-L".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB---CD---EF---GH---IJ---K--L".getBytes();
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    System.arraycopy("K--L".getBytes(), 0, expectedResult, 0, "K--L".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB---CD---EF---GH---IJ----".getBytes();
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[10];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[10];
    expectedResult[0] = '-';
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithOffsetAndLength() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithOffsetAndLength"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SpatialPartition(20, 2),
      ValueType.String, 18);

    byte[] value1 = "AB---CD---EF---GH---IJ---KL---MN---OP---".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[2];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[2];
    System.arraycopy("IJ".getBytes(), 0, expectedResult, 0, "IJ".getBytes().length);
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithOffsetAndLengthWhenPutIsSmallerThanOffset() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(
            TableName.valueOf("testIndexPutWithOffsetAndLengthWhenPutIsSmallerThanOffset"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SpatialPartition(20, 2),
      ValueType.String, 18);

    byte[] value1 = "AB---CD---EF---GH".getBytes();
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] indexRowKey = indexPut.getRow();
    byte[] actualResult = new byte[2];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    byte[] expectedResult = new byte[2];
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));

    value1 = "AB---CD---EF---GH---I".getBytes();
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    indexRowKey = indexPut.getRow();
    actualResult = new byte[2];
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    expectedResult = new byte[2];
    expectedResult[0] = 'I';
    Assert.assertTrue(Bytes.equals(actualResult, expectedResult));
  }

  @Test(timeout = 180000)
  public void testExtentedParametersValidityFailScenarios() throws IOException {
    IndexSpecification spec = new IndexSpecification("index");

    // When separator length is zero
    try {
      spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("", 4),
        ValueType.String, 10);
      Assert.fail("Testcase should fail if separator length is zero.");
    } catch (IllegalArgumentException e) {
    }

    // when the valuePosition is zero with separator
    try {
      spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("--", 0),
        ValueType.String, 10);
      Assert
          .fail("the testcase should fail if the valuePosition with the separator is passed as zero.");
    } catch (IllegalArgumentException e) {
    }

  }

  public void testExtentedParametersValidityPassScenarios() throws IOException {
    IndexSpecification spec = new IndexSpecification("index");

    // When the provided arguments are correct
    try {
      spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SpatialPartition(4, 10),
        ValueType.String, 10);
    } catch (IllegalArgumentException e) {
      Assert.fail("the testcase should not throw exception as the arguments passed are correct.");
    }

    // When the provided arguments are correct
    try {
      spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("--", 1),
        ValueType.String, 10);
    } catch (IllegalArgumentException e) {
      Assert.fail("the testcase should not throw exception as the arguments passed are correct.");
    }

    try {
      spec.addIndexColumn(new HColumnDescriptor("col"), "ql2", ValueType.String, 10);
    } catch (IllegalArgumentException e) {
      Assert.fail("the testcase should not throw exception as the arguments passed are correct.");
    }
  }

  @Test(timeout = 180000)
  public void testColumnQualifierSerialization() throws Exception {
    ByteArrayOutputStream bos = null;
    DataOutputStream dos = null;
    ByteArrayInputStream bis = null;
    DataInputStream dis = null;
    try {
      bos = new ByteArrayOutputStream();
      dos = new DataOutputStream(bos);
      ColumnQualifier cq =
          new ColumnQualifier("cf", "cq", ValueType.String, 10, new SpatialPartition(0, 5));
      cq.write(dos);
      dos.flush();
      byte[] byteArray = bos.toByteArray();
      bis = new ByteArrayInputStream(byteArray);
      dis = new DataInputStream(bis);
      ColumnQualifier c = new ColumnQualifier();
      c.readFields(dis);
      assertTrue("ColumnQualifier state mismatch.", c.equals(cq));
    } finally {
      if (null != bos) {
        bos.close();
      }
      if (null != dos) {
        dos.close();
      }
      if (null != bis) {
        bis.close();
      }
      if (null != dis) {
        dis.close();
      }
    }

  }

  @Test(timeout = 180000)
  public void testIndexPutwithPositiveIntDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutwithPositiveIntDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Int, 4);
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql2", ValueType.Float, 4);

    byte[] value1 = Bytes.toBytes(1000);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut1 = IndexUtils.prepareIndexPut(p, spec, region);
    int a = 1000;
    byte[] expectedResult = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut1.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithNegativeIntDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithNegativeIntDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Int, 4);
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql2", ValueType.Float, 4);

    byte[] value1 = Bytes.toBytes(-2562351);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    int a = -2562351;
    byte[] expectedResult = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithLongDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("testIndexPutWithNegativeIntDataTypes");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Long, 4);

    byte[] value1 = Bytes.toBytes(-2562351L);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    long a = -2562351L;
    byte[] expectedResult = Bytes.toBytes(a ^ (1L << 63));
    byte[] actualResult = new byte[8];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithShortDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("testIndexPutWithNegativeIntDataTypes");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Short, 4);

    short s = 1000;
    byte[] value1 = Bytes.toBytes(s);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult = Bytes.toBytes(s);
    expectedResult[0] ^= 1 << 7;
    byte[] actualResult = new byte[2];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithByteDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("testIndexPutWithNegativeIntDataTypes");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Short, 4);

    byte b = 100;
    byte[] value1 = Bytes.toBytes(b);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult = Bytes.toBytes(b);
    expectedResult[0] ^= 1 << 7;
    byte[] actualResult = new byte[2];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithCharDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd = new HTableDescriptor("testIndexPutWithNegativeIntDataTypes");
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Char, 4);

    char c = 'A';
    byte[] value1 = new byte[2];
    value1[1] = (byte) c;
    c >>= 8;
    value1[0] = (byte) c;
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] actualResult = new byte[2];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(value1, actualResult));
  }

  @Test(timeout = 180000)
  public void testIndexPutWithDoubleDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithNegativeIntDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    // HLog hlog = UTIL.getMiniHBaseCluster().getRegionServer(0).getWAL();
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Double, 8);

    byte[] value1 = Bytes.toBytes(109.4548957D);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    double d = 109.4548957D;
    byte[] expectedResult = Bytes.toBytes(d);
    expectedResult[0] ^= 1 << 7;
    byte[] actualResult = new byte[8];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));

    value1 = Bytes.toBytes(-109.4548957D);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    d = -109.4548957D;
    expectedResult = Bytes.toBytes(d);
    for (int i = 0; i < 8; i++) {
      expectedResult[i] ^= 0xff;
    }
    actualResult = new byte[8];
    indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

  @Test(timeout = 180000)
  public void testSequenceOfIndexPutsWithNegativeInteger() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testSequenceOfIndexPutsWithDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Int, 4);

    byte[] value1 = Bytes.toBytes(-1000);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    int a = -1000;
    byte[] expectedResult = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));

    value1 = Bytes.toBytes(-1500);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut1 = IndexUtils.prepareIndexPut(p, spec, region);
    a = -1500;
    byte[] expectedResult1 = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult1 = new byte[4];
    byte[] indexRowKey1 = indexPut1.getRow();
    System.arraycopy(indexRowKey1, 22, actualResult1, 0, actualResult1.length);
    Assert.assertTrue(Bytes.equals(expectedResult1, actualResult1));

    Assert.assertTrue(Bytes.compareTo(indexPut.getRow(), indexPut1.getRow()) > 0);

    value1 = Bytes.toBytes(1500);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut2 = IndexUtils.prepareIndexPut(p, spec, region);
    a = 1500;
    byte[] expectedResult2 = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult2 = new byte[4];
    byte[] indexRowKey2 = indexPut2.getRow();
    System.arraycopy(indexRowKey2, 22, actualResult2, 0, actualResult2.length);
    Assert.assertTrue(Bytes.equals(expectedResult2, actualResult2));

    Assert.assertTrue(Bytes.compareTo(indexPut2.getRow(), indexPut.getRow()) > 0);

    value1 = Bytes.toBytes(2000);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut3 = IndexUtils.prepareIndexPut(p, spec, region);
    a = 2000;
    byte[] expectedResult3 = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult3 = new byte[4];
    byte[] indexRowKey3 = indexPut3.getRow();
    System.arraycopy(indexRowKey3, 22, actualResult3, 0, actualResult3.length);
    Assert.assertTrue(Bytes.equals(expectedResult3, actualResult3));

    Assert.assertTrue(Bytes.compareTo(indexPut3.getRow(), indexPut2.getRow()) > 0);
  }

  @Test(timeout = 180000)
  public void testSequenceOfIndexPutsWithNegativeFloat() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testSequenceOfIndexPutsWithDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Float, 4);

    byte[] value1 = Bytes.toBytes(-10.40f);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult = Bytes.toBytes(-10.40f);
    expectedResult[0] ^= 0xff;
    expectedResult[1] ^= 0xff;
    expectedResult[2] ^= 0xff;
    expectedResult[3] ^= 0xff;
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));

    value1 = Bytes.toBytes(-15.20f);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut1 = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult1 = Bytes.toBytes(-15.20f);
    expectedResult1[0] ^= 0xff;
    expectedResult1[1] ^= 0xff;
    expectedResult1[2] ^= 0xff;
    expectedResult1[3] ^= 0xff;
    byte[] actualResult1 = new byte[4];
    byte[] indexRowKey1 = indexPut1.getRow();
    System.arraycopy(indexRowKey1, 22, actualResult1, 0, actualResult1.length);
    Assert.assertTrue(Bytes.equals(expectedResult1, actualResult1));

    Assert.assertTrue(Bytes.compareTo(indexPut.getRow(), indexPut1.getRow()) > 0);

    value1 = Bytes.toBytes(30.50f);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut2 = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult2 = Bytes.toBytes(30.50f);
    expectedResult2[0] ^= 1 << 7;
    byte[] actualResult2 = new byte[4];
    byte[] indexRowKey2 = indexPut2.getRow();
    System.arraycopy(indexRowKey2, 22, actualResult2, 0, actualResult2.length);
    Assert.assertTrue(Bytes.equals(expectedResult2, actualResult2));

    Assert.assertTrue(Bytes.compareTo(indexPut2.getRow(), indexPut.getRow()) > 0);

    value1 = Bytes.toBytes(40.54f);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut3 = IndexUtils.prepareIndexPut(p, spec, region);
    byte[] expectedResult3 = Bytes.toBytes(40.54f);
    expectedResult3[0] ^= 1 << 7;
    byte[] actualResult3 = new byte[4];
    byte[] indexRowKey3 = indexPut3.getRow();
    System.arraycopy(indexRowKey3, 22, actualResult3, 0, actualResult3.length);
    Assert.assertTrue(Bytes.equals(expectedResult3, actualResult3));

    Assert.assertTrue(Bytes.compareTo(indexPut3.getRow(), indexPut2.getRow()) > 0);
  }

  @Test(timeout = 180000)
  public void testSequenceOfIndexPutsWithDataTypes() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testSequenceOfIndexPutsWithDataTypes"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", ValueType.Int, 4);

    byte[] value1 = Bytes.toBytes(1000);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    int a = 1000;
    byte[] expectedResult = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));

    value1 = Bytes.toBytes(-2562351);
    p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), value1);
    Put indexPut1 = IndexUtils.prepareIndexPut(p, spec, region);
    a = -2562351;
    byte[] expectedResult1 = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult1 = new byte[4];
    byte[] indexRowKey1 = indexPut1.getRow();
    System.arraycopy(indexRowKey1, 22, actualResult1, 0, actualResult1.length);
    Assert.assertTrue(Bytes.equals(expectedResult1, actualResult1));

    Assert.assertTrue(Bytes.compareTo(indexPut.getRow(), indexPut1.getRow()) > 0);
  }

  @Test(timeout = 180000)
  public void testIndexPutWithSeparatorAndDataType() throws IOException {
    Path basedir = new Path(DIR + "TestIndexPut");
    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf("testIndexPutWithSeparatorAndDataType"));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), "ABC".getBytes(), "BBB".getBytes(), false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    IndexSpecification spec = new IndexSpecification("index");
    spec.addIndexColumn(new HColumnDescriptor("col"), "ql1", new SeparatorPartition("---", 4),
      ValueType.Int, 4);

    byte[] putValue = new byte[19];
    byte[] value1 = "AB---CD---EF---".getBytes();
    byte[] value2 = Bytes.toBytes(100000);
    System.arraycopy(value1, 0, putValue, 0, value1.length);
    System.arraycopy(value2, 0, putValue, value1.length, value2.length);
    Put p = new Put("row".getBytes());
    p.add("col".getBytes(), "ql1".getBytes(), putValue);
    Put indexPut = IndexUtils.prepareIndexPut(p, spec, region);
    int a = 100000;
    byte[] expectedResult = Bytes.toBytes(a ^ (1 << 31));
    byte[] actualResult = new byte[4];
    byte[] indexRowKey = indexPut.getRow();
    System.arraycopy(indexRowKey, 22, actualResult, 0, actualResult.length);
    Assert.assertTrue(Bytes.equals(expectedResult, actualResult));
  }

}
