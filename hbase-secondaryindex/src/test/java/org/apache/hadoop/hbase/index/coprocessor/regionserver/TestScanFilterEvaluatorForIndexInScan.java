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

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.GroupingCondition;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.client.EqualsExpression;
import org.apache.hadoop.hbase.index.client.IndexExpression;
import org.apache.hadoop.hbase.index.client.IndexUtils;
import org.apache.hadoop.hbase.index.client.MultiIndexExpression;
import org.apache.hadoop.hbase.index.client.NoIndexExpression;
import org.apache.hadoop.hbase.index.client.RangeExpression;
import org.apache.hadoop.hbase.index.client.SingleIndexExpression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestScanFilterEvaluatorForIndexInScan {

  private static final byte[] FAMILY1 = Bytes.toBytes("cf1");
  private static final byte[] FAMILY2 = Bytes.toBytes("cf2");
  private static final String COL1 = "c1";
  private static final String COL2 = "c2";
  private static final String COL3 = "c3";
  private static final byte[] QUALIFIER1 = Bytes.toBytes(COL1);
  private static final byte[] QUALIFIER2 = Bytes.toBytes(COL2);
  private static final byte[] QUALIFIER3 = Bytes.toBytes(COL3);
  private static final String tableName = "tab1";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getDataTestDir(
    "TestScanFilterEvaluatorForIndexInScan").toString();

  @Test
  public void testSingleIndexExpressionWithOneEqualsExpression() throws Exception {
    String indexName = "idx1";
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression(indexName);
    byte[] value = "1".getBytes();
    Column column = new Column(FAMILY1, QUALIFIER1);
    EqualsExpression equalsExpression = new EqualsExpression(column, value);
    singleIndexExpression.addEqualsExpression(equalsExpression);

    Scan scan = new Scan();
    scan.setAttribute(Constants.INDEX_EXPRESSION, IndexUtils.toBytes(singleIndexExpression));
    Filter filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER1, CompareOp.EQUAL, value);
    scan.setFilter(filter);
    ScanFilterEvaluator evaluator = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    IndexSpecification index = new IndexSpecification(indexName);
    HColumnDescriptor colDesc = new HColumnDescriptor(FAMILY1);
    index.addIndexColumn(colDesc, COL1, ValueType.String, 10);
    indices.add(index);
    HRegion region =
        initHRegion(tableName.getBytes(), null, null,
          "testSingleIndexExpressionWithOneEqualsExpression", TEST_UTIL.getConfiguration(), FAMILY1);
    IndexRegionScanner scanner = evaluator.evaluate(scan, indices, new byte[0], region, tableName);
    // TODO add assertions
  }

  @Test
  public void testSingleIndexExpressionWithMoreEqualsExpsAndOneRangeExp() throws Exception {
    String indexName = "idx1";
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression(indexName);
    byte[] value1 = "1".getBytes();
    byte[] value2 = Bytes.toBytes(1234);
    Column column = new Column(FAMILY1, QUALIFIER1);
    EqualsExpression equalsExpression = new EqualsExpression(column, value1);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    column = new Column(FAMILY1, QUALIFIER2);
    equalsExpression = new EqualsExpression(column, value2);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    column = new Column(FAMILY1, QUALIFIER3);
    byte[] value3_1 = Bytes.toBytes(10.4F);
    byte[] value3_2 = Bytes.toBytes(16.91F);
    RangeExpression re = new RangeExpression(column, value3_1, value3_2, true, false);
    singleIndexExpression.setRangeExpression(re);

    Scan scan = new Scan();
    scan.setAttribute(Constants.INDEX_EXPRESSION, IndexUtils.toBytes(singleIndexExpression));
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
    Filter filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER1, CompareOp.EQUAL, value1);
    fl.addFilter(filter);
    filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER2, CompareOp.EQUAL, value2);
    fl.addFilter(filter);
    filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER3, CompareOp.GREATER_OR_EQUAL, value3_1);
    fl.addFilter(filter);
    filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER3, CompareOp.LESS, value3_2);
    fl.addFilter(filter);
    scan.setFilter(fl);

    ScanFilterEvaluator evaluator = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    IndexSpecification index = new IndexSpecification(indexName);
    HColumnDescriptor colDesc = new HColumnDescriptor(FAMILY1);
    index.addIndexColumn(colDesc, COL1, ValueType.String, 10);
    index.addIndexColumn(colDesc, COL2, ValueType.Int, 4);
    index.addIndexColumn(colDesc, COL3, ValueType.Float, 4);
    indices.add(index);

    HRegion region =
        initHRegion(tableName.getBytes(), null, null,
          "testSingleIndexExpressionWithMoreEqualsExpsAndOneRangeExp",
          TEST_UTIL.getConfiguration(), FAMILY1);
    IndexRegionScanner scanner = evaluator.evaluate(scan, indices, new byte[0], region, tableName);
    // TODO add assertions
  }

  @Test
  public void testMultiIndexExpression() throws Exception {
    MultiIndexExpression multiIndexExpression = new MultiIndexExpression(GroupingCondition.AND);
    String index1 = "idx1";
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression(index1);
    byte[] value2 = Bytes.toBytes(1234);
    Column column = new Column(FAMILY1, QUALIFIER2);
    EqualsExpression equalsExpression = new EqualsExpression(column, value2);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    column = new Column(FAMILY1, QUALIFIER3);
    byte[] value3_1 = Bytes.toBytes(10.4F);
    byte[] value3_2 = Bytes.toBytes(16.91F);
    RangeExpression re = new RangeExpression(column, value3_1, value3_2, true, false);
    singleIndexExpression.setRangeExpression(re);
    multiIndexExpression.addIndexExpression(singleIndexExpression);

    MultiIndexExpression multiIndexExpression2 = new MultiIndexExpression(GroupingCondition.OR);
    String index2 = "idx2";
    singleIndexExpression = new SingleIndexExpression(index2);
    byte[] value1 = Bytes.toBytes("asdf");
    column = new Column(FAMILY1, QUALIFIER1);
    equalsExpression = new EqualsExpression(column, value1);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    multiIndexExpression2.addIndexExpression(singleIndexExpression);

    String index3 = "idx3";
    singleIndexExpression = new SingleIndexExpression(index3);
    byte[] value4 = Bytes.toBytes(567.009D);
    column = new Column(FAMILY2, QUALIFIER1);
    equalsExpression = new EqualsExpression(column, value4);
    singleIndexExpression.addEqualsExpression(equalsExpression);
    multiIndexExpression2.addIndexExpression(singleIndexExpression);

    multiIndexExpression.addIndexExpression(multiIndexExpression2);

    Scan scan = new Scan();
    scan.setAttribute(Constants.INDEX_EXPRESSION, IndexUtils.toBytes(multiIndexExpression));
    FilterList outerFL = new FilterList(Operator.MUST_PASS_ALL);
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
    Filter filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER2, CompareOp.EQUAL, value2);
    fl.addFilter(filter);
    filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER3, CompareOp.GREATER_OR_EQUAL, value3_1);
    fl.addFilter(filter);
    filter = new SingleColumnValueFilter(FAMILY1, QUALIFIER3, CompareOp.LESS, value3_2);
    fl.addFilter(filter);
    outerFL.addFilter(fl);
    FilterList innerFL = new FilterList(Operator.MUST_PASS_ONE);
    innerFL.addFilter(new SingleColumnValueFilter(FAMILY1, QUALIFIER1, CompareOp.EQUAL, value1));
    innerFL.addFilter(new SingleColumnValueFilter(FAMILY2, QUALIFIER1, CompareOp.EQUAL, value4));
    outerFL.addFilter(innerFL);
    scan.setFilter(outerFL);

    ScanFilterEvaluator evaluator = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    IndexSpecification is1 = new IndexSpecification(index1);
    HColumnDescriptor colDesc = new HColumnDescriptor(FAMILY1);
    is1.addIndexColumn(colDesc, COL2, ValueType.Int, 4);
    is1.addIndexColumn(colDesc, COL3, ValueType.Float, 4);
    indices.add(is1);
    IndexSpecification is2 = new IndexSpecification(index2);
    is2.addIndexColumn(colDesc, COL1, ValueType.String, 15);
    indices.add(is2);
    IndexSpecification is3 = new IndexSpecification(index3);
    colDesc = new HColumnDescriptor(FAMILY2);
    is3.addIndexColumn(colDesc, COL1, ValueType.Double, 8);
    indices.add(is3);

    HRegion region =
        initHRegion(tableName.getBytes(), null, null, "testMultiIndexExpression",
          TEST_UTIL.getConfiguration(), FAMILY1);
    IndexRegionScanner scanner = evaluator.evaluate(scan, indices, new byte[0], region, tableName);
    // TODO add assertions
  }

  @Test
  public void testNoIndexExpression() throws Exception {
    IndexExpression exp = new NoIndexExpression();
    Scan scan = new Scan();
    scan.setAttribute(Constants.INDEX_EXPRESSION, IndexUtils.toBytes(exp));
    byte[] value1 = Bytes.toBytes("asdf");
    scan.setFilter(new SingleColumnValueFilter(FAMILY1, QUALIFIER1, CompareOp.EQUAL, value1));
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    IndexSpecification is1 = new IndexSpecification("idx1");
    HColumnDescriptor colDesc = new HColumnDescriptor(FAMILY1);
    is1.addIndexColumn(colDesc, COL1, ValueType.String, 15);
    indices.add(is1);
    ScanFilterEvaluator evaluator = new ScanFilterEvaluator();
    HRegion region =
        initHRegion(tableName.getBytes(), null, null, "testNoIndexExpression",
          TEST_UTIL.getConfiguration(), FAMILY1);
    IndexRegionScanner scanner = evaluator.evaluate(scan, indices, new byte[0], region, tableName);
    assertNull(scanner);
  }

  private static HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getTableName(), startKey, stopKey, false);
    Path path = new Path(DIR + callingMethod);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    return HRegion.createHRegion(info, path, conf, htd);
  }
}
