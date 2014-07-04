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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.filter.SingleColumnRangeFilter;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestScanFilterEvaluator extends TestCase {

/*  public void testCircularList() throws Exception{
    ScanFilterEvaluator.CirularDoublyLinkedList cir = new ScanFilterEvaluator.CirularDoublyLinkedList();
    cir.add(10);
    cir.add(23);
    cir.add(45);
    Node head = cir.getHead();
    //System.out.println(head);
    Node next = head;
    for(int i = 0; i< 100; i++){
      next = cir.next(next);
      System.out.println(next);
    }
    
  }*/
  
/*  public void testName1() throws Exception {
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1", "c2" }, "idx1"));
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1" }, "idx2"));
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2" }, "idx3"));
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c1" }, "idx4"));
    
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.EQUAL, "a".getBytes());
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);
    
    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c3".getBytes(), CompareOp.EQUAL, "a".getBytes());
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c4".getBytes(), CompareOp.EQUAL, "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);
    
    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c5".getBytes(), CompareOp.EQUAL, "a".getBytes());
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c6".getBytes(), CompareOp.GREATER, "K".getBytes());
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);
    
    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
   // mapper.evaluate(masterFilter, indices);
  }*/
  private static final String COLUMN_FAMILY = "MyCF";

  static HRegion region = null;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getDataTestDir("TestScanFilterEvaluator").toString();

  static String method = "TestScanFilterEvaluator";
  static byte[] tableName = Bytes.toBytes(method);
  static byte[] family = Bytes.toBytes("family");
  static byte[] qual = Bytes.toBytes("q");
  private final int MAX_VERSIONS = 2;

  private static HRegion initHRegion(byte[] tableName, String callingMethod, Configuration conf,
      byte[]... families) throws IOException {
    return initHRegion(tableName, null, null, callingMethod, conf, families);
  }

  /**
   * @param tableName
   * @param startKey
   * @param stopKey
   * @param callingMethod
   * @param conf
   * @param families
   * @throws IOException
   * @return A region on which you must call {@link HRegion#closeHRegion(HRegion)} when done.
   */
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

  public void testName2() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, method, conf, family);
    ArrayList<IndexSpecification> arrayList = new ArrayList<IndexSpecification>();
    List<IndexSpecification> indices = arrayList;
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1", "c2",
        "c3", "c4", "c5", "c6", "c7" }, "idx1"));
    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1" }, "idx2"));
    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2" }, "idx3"));
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c1" },
      "idx4"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    SingleColumnValueFilter iscvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c7".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);
    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    // Will throw null pointer here.
    IndexSpecification indexSpec = new IndexSpecification("a");
    indexSpec
        .addIndexColumn(new HColumnDescriptor(family), Bytes.toString(qual), ValueType.Int, 10);
    boolean add = arrayList.add(indexSpec);
    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      arrayList);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testDiffCombinations() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "testDiffCombinations", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();

    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c3",
        "c4", "c5", "c6" }, "idx1"));

    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c1",
        "c3", "c4" }, "idx4"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    filter2.addFilter(iscvf1);

    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      indices);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testDiffCombinations1() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "testDiffCombinations1", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c3",
        "c4", }, "idx1"));

    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c1" },
      "idx2"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c5" }, "idx3"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    filter2.addFilter(iscvf1);

    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);
    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      indices);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testDiffCombinations2() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "testDiffCombinations2", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c3" },
      "idx1"));

    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c1" },
      "idx2"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c5" }, "idx3"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    filter2.addFilter(iscvf1);

    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      indices);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testDiffCombinations3() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "testDiffCombinations3", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c3" },
      "idx1"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1" }, "idx2"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c5" }, "idx3"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c4" }, "idx4"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c6" }, "idx5"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    filter2.addFilter(iscvf1);

    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      indices);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testWhenORWithSameColumnAppearsinDiffChild() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "tesWhenORWithSameColumnAppearsinDiffChild", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    // create the indices.
    indices.add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c2", "c3" },
      "idx1"));

    indices
        .add(createIndexSpecification("cf1", ValueType.String, 10, new String[] { "c1" }, "idx2"));

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    SingleColumnValueFilter iscvf3 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "d".getBytes());
    // filter2.addFilter(iscvf3);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(iscvf3);
    Scan scan = new Scan();
    scan.setFilter(masterFilter);

    IndexManager.getInstance().addIndexForTable(this.region.getTableDesc().getNameAsString(),
      indices);
    mapper.evaluate(scan, indices, new byte[0], this.region, this.region.getTableDesc()
        .getNameAsString());
  }

  public void testWhenORConditionAppears() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    region = initHRegion(tableName, "testWhenORConditionAppears", conf, family);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c2".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
            "a".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c4".getBytes(), CompareOp.EQUAL,
            "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c5".getBytes(), CompareOp.GREATER,
            Bytes.toBytes(10));
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c6".getBytes(), CompareOp.EQUAL,
            Bytes.toBytes(100));
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    // System.out.println(doFiltersRestruct);
    /*
     * if (doFiltersRestruct instanceof FilterList) { FilterList list = ((FilterList)
     * doFiltersRestruct); assertEquals(3, list.getFilters().size()); }
     */
  }

  public void testORFiltersGrouping() throws Exception {
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    SingleColumnValueFilter iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    SingleColumnValueFilter iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    Filter resultFilter = mapper.doFiltersRestruct(masterFilter);
    List<Filter> filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnValueFilter);
    assertTrue(((SingleColumnValueFilter) filterList.get(0)).getOperator().equals(CompareOp.EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));
    assertTrue(((SingleColumnRangeFilter) filterList.get(1)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));
    assertTrue(((SingleColumnRangeFilter) filterList.get(1)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));
    assertTrue(((SingleColumnRangeFilter) filterList.get(1)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));
    assertTrue(((SingleColumnRangeFilter) filterList.get(1)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "10".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "10".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(filterList.get(1) instanceof SingleColumnValueFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));
    assertTrue(((SingleColumnValueFilter) filterList.get(1)).getOperator().equals(CompareOp.EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "9".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(filterList.get(1) instanceof SingleColumnValueFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));
    assertTrue(((SingleColumnValueFilter) filterList.get(1)).getOperator().equals(CompareOp.EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "9".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 2);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(filterList.get(1) instanceof SingleColumnValueFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));
    assertTrue(((SingleColumnValueFilter) filterList.get(1)).getOperator().equals(CompareOp.EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "7".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "7".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "2".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "7".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
            "7".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getUpperBoundOp().equals(
      CompareOp.LESS_OR_EQUAL));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER));

    masterFilter = new FilterList(Operator.MUST_PASS_ONE);
    iscvf1 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "3".getBytes());
    iscvf2 =
        new SingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER_OR_EQUAL,
            "5".getBytes());
    masterFilter.addFilter(iscvf1);
    masterFilter.addFilter(iscvf2);
    resultFilter = mapper.doFiltersRestruct(masterFilter);
    filterList = ((FilterList) resultFilter).getFilters();
    assertTrue(filterList.size() == 1);
    assertTrue(filterList.get(0) instanceof SingleColumnRangeFilter);
    assertTrue(((SingleColumnRangeFilter) filterList.get(0)).getLowerBoundOp().equals(
      CompareOp.GREATER_OR_EQUAL));

  }

  /*public void testWhenColNamesAreRepeated() throws Exception {
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();

    
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.EQUAL, "a".getBytes());
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.EQUAL, "K".getBytes());
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);
    
    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c3".getBytes(), CompareOp.EQUAL, "a".getBytes());
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c4".getBytes(), CompareOp.EQUAL, "K".getBytes());
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);
        
    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    
    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(3, list.getFilters().size());
      
      List<Filter> filters = list.getFilters();
      for (Filter filter2 : filters) {
        if(filter2 instanceof SingleColumnValueFilter){
          SingleColumnValueFilter scvf = (SingleColumnValueFilter)filter2;
          if(Bytes.equals(scvf.getQualifier(), "c1".getBytes())){
            assertTrue(Bytes.equals("K".getBytes(), scvf.getComparator().getValue()));
          }
        }
      }
    }
  }
  
  public void testWhenSameColsConditionsComeMoreThanOnce() throws Exception{

    
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.GREATER, Bytes.toBytes(20));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.LESS, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);
    
    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c3".getBytes(), CompareOp.EQUAL, Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c4".getBytes(), CompareOp.EQUAL, Bytes.toBytes(100));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);
    
    FilterList filter2 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.GREATER, Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c4".getBytes(), CompareOp.EQUAL, Bytes.toBytes(100));
    filter2.addFilter(iscvf1);
    filter2.addFilter(iscvf2);
        
    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);
    masterFilter.addFilter(filter2);
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(2, list.getFilters().size());
    }
    
  }
  
  public void testShouldTakeOnlyTheEqualConditionWhenGreaterAlsoComes() throws Exception {
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();

    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.GREATER, Bytes.toBytes(20));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(100));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);

    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(3, list.getFilters().size());
    }
  }
  
  public void testShouldTakeOnlyTheEqualConditionWhenLesserAlsoComes() throws Exception{
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
        Bytes.toBytes(100));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);

    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(3, list.getFilters().size());
      List<Filter> filters = list.getFilters();
      for (Filter filter2 : filters) {
        if (filter2 instanceof SingleColumnValueFilter) {
          SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter2;
          if (Bytes.equals(scvf.getQualifier(), "c1".getBytes())) {
            assertEquals(CompareOp.EQUAL, scvf.getOperator());
          }
        }
      }
    }
  }
  
  public void testShouldNotIncludeFilterIfTheRangeConditionIsWrong() throws Exception{
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.LESS, Bytes.toBytes(10));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
        Bytes.toBytes(100));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);

    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(2, list.getFilters().size());
    }
  }
  
  public void testShouldTakeOnlyTheHighestFilterWhenTwoGreaterConditonsAreFound() throws Exception{
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.GREATER, Bytes.toBytes(10));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.GREATER,
        Bytes.toBytes(100));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);

    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(3, list.getFilters().size());
      List<Filter> filters = list.getFilters();
      for (Filter filter2 : filters) {
        if (filter2 instanceof SingleColumnValueFilter) {
          SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter2;
          if (Bytes.equals(scvf.getQualifier(), "c1".getBytes())) {
            assertTrue(Bytes.equals(Bytes.toBytes(100), scvf.getComparator().getValue()));
          }
        }
      }
    }
  }
  
  public void testShouldTakeOnlyTheLowestFilterWhenTwoLesserConditonsAreFound() throws Exception{
    ScanFilterEvaluator mapper = new ScanFilterEvaluator();
    FilterList masterFilter = new FilterList(Operator.MUST_PASS_ALL);
    // create the filter
    FilterList filter = new FilterList(Operator.MUST_PASS_ALL);
    IndexedSingleColumnValueFilter iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c1".getBytes(), CompareOp.LESS, Bytes.toBytes(10));
    IndexedSingleColumnValueFilter iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(),
        "c2".getBytes(), CompareOp.EQUAL, Bytes.toBytes(20));
    filter.addFilter(iscvf1);
    filter.addFilter(iscvf2);

    FilterList filter1 = new FilterList(Operator.MUST_PASS_ALL);
    iscvf1 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c3".getBytes(), CompareOp.EQUAL,
        Bytes.toBytes(10));
    iscvf2 = new IndexedSingleColumnValueFilter("cf1".getBytes(), "c1".getBytes(), CompareOp.LESS,
        Bytes.toBytes(1));
    filter1.addFilter(iscvf1);
    filter1.addFilter(iscvf2);

    masterFilter.addFilter(filter);
    masterFilter.addFilter(filter1);

    Filter doFiltersRestruct = mapper.doFiltersRestruct(masterFilter);
    if (doFiltersRestruct instanceof FilterList) {
      FilterList list = ((FilterList) doFiltersRestruct);
      assertEquals(3, list.getFilters().size());
      List<Filter> filters = list.getFilters();
      for (Filter filter2 : filters) {
        if (filter2 instanceof SingleColumnValueFilter) {
          SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter2;
          if (Bytes.equals(scvf.getQualifier(), "c1".getBytes())) {
            assertTrue(Bytes.equals(Bytes.toBytes(1), scvf.getComparator().getValue()));
          }
        }
      }
    }
  }
*/
  private IndexSpecification createIndexSpecification(String cf, ValueType type,
      int maxValueLength, String[] qualifiers, String name) {
    IndexSpecification index = new IndexSpecification(name.getBytes());
    for (String qualifier : qualifiers) {
      index.addIndexColumn(new HColumnDescriptor(cf), qualifier, type, maxValueLength);
    }
    return index;
  }

}
