package org.apache.hadoop.hbase.index.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIndexAdmin {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static IndexAdmin admin = null;
  private static AtomicInteger count = new AtomicInteger(0);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, MockedRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
      LoadBalancer.class);
    conf.setInt("hbase.hstore.compactionThreshold",5);
    UTIL.startMiniCluster(1);
    admin = new IndexAdmin(conf);
  }
  @Test
  public void testAddIndexAPI() {
    try {
      admin.addIndex(null, null);
      fail("addIndex should through exception.");
    } catch (Exception e) { }
    
    try {
      admin.addIndex(TableName.valueOf("test"), null);
      fail("addIndex should through exception.");
    } catch (Exception e) { }
    
    try {
      admin.addIndex(null, new IndexSpecification("idx"));
      fail("addIndex should through exception.");
    } catch (Exception e) { }
    

    try {
      admin.addIndexes(null, null);
      fail("addIndexes should through exception.");
    } catch (Exception e) { }
    
    try {
      admin.addIndexes(TableName.valueOf("test"), null);
      fail("addIndexes should through exception.");
    } catch (Exception e) { }
    
    try {
      admin.addIndexes(null, new ArrayList<IndexSpecification>());
      fail("addIndexes should through exception.");
    } catch (Exception e) { }

    try {
      admin.addIndexes(TableName.valueOf("test"), new ArrayList<IndexSpecification>());
      fail("addIndexes should through exception.");
    } catch (Exception e) { }
    
    try {
      admin.getConfiguration().set("hbase.use.secondary.index", "false");
      admin.addIndex(TableName.valueOf("test"), new IndexSpecification("idx"));
      fail("addIndexes should through exception.");
    } catch (Exception e) { 
      assertTrue(e.getMessage().contains("hbase.use.secondary.index"));
    } finally {
      admin.getConfiguration().set("hbase.use.secondary.index", "true");
    }

  }
  
  @Test(timeout = 180000)
  public void testAddIndex() throws IOException, KeeperException,
 InterruptedException {
    Configuration conf = UTIL.getConfiguration();

    String userTableName = "testAddIndex";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(userTableName));
    IndexSpecification iSpec = new IndexSpecification("Index1");
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    htd.addFamily(hcd);
    admin.createTable(htd);
    try {
      IndexSpecification spec = new IndexSpecification("idx");
      spec.addIndexColumn(new HColumnDescriptor("unknown"), "q", null, 10);
      admin.addIndex(htd.getTableName(), spec);
      fail("Index should not be added if the column family is not present in the table.");
    } catch (Exception e) {
    }
    admin.addIndex(htd.getTableName(), iSpec);

    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    table.put(p);

    int i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(1, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(1, i);

    // Test put without the indexed column
    Put p1 = new Put("row2".getBytes());
    p1.add("col".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p1);

    i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(2, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(1, i);
    admin.disableTable(userTableName);
    admin.deleteTable(userTableName);
  }

  @Test(timeout = 180000)
  public void testAddIndexWithData() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();

    String userTableName = "testAddIndexWithData";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(userTableName));
    IndexSpecification iSpec = new IndexSpecification("Index1");
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    htd.addFamily(hcd);
    byte[][] splits = {Bytes.toBytes("row1"), Bytes.toBytes("row2"), Bytes.toBytes("row3")};
    admin.createTable(htd, splits);
    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    table.put(p);
    p = new Put("row2".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    table.put(p);
    p = new Put("row3".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    table.put(p);
    admin.addIndex(htd.getTableName(), iSpec);
    int i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(3, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(3, i);

    // Test put without the indexed column
    Put p1 = new Put("row4".getBytes());
    p1.add("col".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p1);

    i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(4, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(3, i);
    admin.disableTable(userTableName);
    admin.deleteTable(userTableName);

  }


  @Test(timeout = 600000)
  public void testAddAndDropIndexesWithData() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();

    String userTableName = "testAddAndDropIndexesWithData";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    htd.addFamily(hcd);
    byte[][] splits =
        { Bytes.toBytes("row"+1), Bytes.toBytes("row"+2), Bytes.toBytes("row"+3), Bytes.toBytes("row"+4), Bytes.toBytes("row"+5),
            Bytes.toBytes("row"+6), Bytes.toBytes("row"+7), Bytes.toBytes("row"+8), Bytes.toBytes("row"+9) };
    admin.createTable(htd, splits);
    for(int j = 0; j< 1; j++) {
      HTable table = new HTable(admin.getConfiguration(), userTableName);
      // test put with the indexed column
      Put p = null;
      for(int i = 0; i< 100; i++) {
        p = new Put(Bytes.toBytes("row" + i));
        p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
        p.add("col".getBytes(), "q2".getBytes(), "myValue".getBytes());
        table.put(p);
      }
      IndexSpecification iSpec = new IndexSpecification("Index1");
      iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
      admin.addIndex(htd.getTableName(), iSpec);
      IndexSpecification iSpec2= new IndexSpecification("Index2");
      hcd = new HColumnDescriptor("col");
      iSpec2.addIndexColumn(hcd, "q2", ValueType.String, 10);
      admin.addIndex(htd.getTableName(), iSpec2);
      int i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
      Assert.assertEquals(100, i);
      i =
          IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
            UTIL.getConfiguration());
      Assert.assertEquals(200, i);
      count.compareAndSet(1, 2);
      admin.dropIndex(htd.getTableName(), iSpec.getName());
      i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
      Assert.assertEquals(100, i);
      i =
          IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
            UTIL.getConfiguration());
      Assert.assertEquals(100, i);
      admin.dropIndex(htd.getTableName(), iSpec2.getName());
      for(int k = 0; k<100; k++) {
        Delete d = new Delete(Bytes.toBytes("row"+k));
        table.delete(d);
      }
      table.close();
    }
    admin.disableTable(userTableName);
    admin.deleteTable(userTableName);

  }
  
  @Test
  public void testAddIndexOnColumnWhoseDataNotPresentInTable() throws IOException, KeeperException, InterruptedException {
    Configuration conf = UTIL.getConfiguration();

    String userTableName = "testAddAndDropIndexesRepeatedly";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    htd.addFamily(hcd);
    byte[][] splits =
        { Bytes.toBytes("row"+1), Bytes.toBytes("row"+2), Bytes.toBytes("row"+3), Bytes.toBytes("row"+4), Bytes.toBytes("row"+5),
            Bytes.toBytes("row"+6), Bytes.toBytes("row"+7), Bytes.toBytes("row"+8), Bytes.toBytes("row"+9) };
    admin.createTable(htd, splits);
    HTable table = new HTable(admin.getConfiguration(), userTableName);
    // test put with the indexed column
    Put p = null;
    for (int i = 0; i < 100; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add("col".getBytes(), "q2".getBytes(), "myValue".getBytes());
      table.put(p);
    }
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    admin.addIndex(htd.getTableName(), iSpec);
    int i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(100, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(0, i);
    table.close();
    admin.disableTable(userTableName);
    admin.deleteTable(userTableName);

  }
  
  @Test(timeout = 180000)
  public void testDropIndex() throws IOException, KeeperException,
      InterruptedException {
    Configuration conf = UTIL.getConfiguration();

    String userTableName = "testDropIndex";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    htd.addFamily(hcd);
    admin.createTable(htd);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    admin.addIndex(htd.getTableName(), iSpec);
    iSpec = new IndexSpecification("Index2");
    iSpec.addIndexColumn(hcd, "q2", ValueType.String, 10);
    admin.addIndex(htd.getTableName(), iSpec);

    HTable table = new HTable(conf, userTableName);
    // test put with the indexed column
    Put p = new Put("row1".getBytes());
    p.add("col".getBytes(), "ql".getBytes(), "myValue".getBytes());
    p.add("col".getBytes(), "q2".getBytes(), "myValue".getBytes());
    table.put(p);

    int i = IndexUtils.countNumberOfRows(userTableName, UTIL.getConfiguration());
    Assert.assertEquals(1, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(2, i);

    // Test put without the indexed column
    Put p1 = new Put("row2".getBytes());
    p1.add("col".getBytes(), "ql1".getBytes(), "myValue".getBytes());
    table.put(p1);

    i = IndexUtils.countNumberOfRows(userTableName,UTIL.getConfiguration());
    Assert.assertEquals(2, i);
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(2, i);
    admin.dropIndex(TableName.valueOf(userTableName), "Index1");
    i =
        IndexUtils.countNumberOfRows(userTableName + Constants.INDEX_TABLE_SUFFIX,
          UTIL.getConfiguration());
    Assert.assertEquals(1, i);
    admin.dropIndex(TableName.valueOf(userTableName), "Index2");
    assertFalse(admin.tableExists(userTableName + Constants.INDEX_TABLE_SUFFIX));
  }

  @Test(timeout = 180000)
  public void testDropIndexShouldNotPutEntriesIntoIndexTableAfterDropping() throws Exception {
    String userTableName = "testDropIndexShouldNotPutEntriesIntoIndexTableAfterDropping";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    HColumnDescriptor hcd1 = new HColumnDescriptor("col2");
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    byte[][] splits = {Bytes.toBytes("row1"), Bytes.toBytes("row2"), Bytes.toBytes("row3")};
    admin.createTable(ihtd, splits);
    IndexSpecification iSpec = new IndexSpecification("Index1");
    IndexSpecification iSpec1 = new IndexSpecification("Index2");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>(2);
    indices.add(iSpec);
    indices.add(iSpec1);
    admin.addIndexes(TableName.valueOf(userTableName),indices);
    HTable table = new HTable(admin.getConfiguration(), userTableName);
    // test put with the indexed column
    for (int i = 0; i < 3; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col1".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    for (int i = 3; i < 5; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col2".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    admin.dropIndex(TableName.valueOf(userTableName), "Index2");
    for (int i = 5; i < 10; i++) {
      String row = "row" + i;
      Put p = new Put(row.getBytes());
      String val = "Val" + i;
      p.add("col2".getBytes(), "ql".getBytes(), val.getBytes());
      table.put(p);
    }
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    int mainTableCount = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      mainTableCount++;
    }
    Assert.assertEquals(10, mainTableCount);

    Scan s1 = new Scan();
    HTable tableidx = new HTable(admin.getConfiguration(), userTableName + "_idx");
    ResultScanner scanner1 = tableidx.getScanner(s1);
    int indexTableCount = 0;
    for (Result rr = scanner1.next(); rr != null; rr = scanner1.next()) {
      indexTableCount++;
      Assert.assertFalse(Bytes.toString(rr.getRow()).contains("row6"));
    }
    Assert.assertEquals(3, indexTableCount);

  }
  
  @Test(timeout = 180000)
  public void testDropIndexAfterDroppingIndexShouldNotGoThruIndexedFlowDuringScan()
      throws Exception {
    String userTableName = "testDropIndexWithScan";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    HColumnDescriptor hcd1 = new HColumnDescriptor("col2");
    HColumnDescriptor hcd3 = new HColumnDescriptor("col3");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    IndexSpecification iSpec1 = new IndexSpecification("Index2");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    iSpec1.addIndexColumn(hcd1, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    ihtd.addFamily(hcd1);
    ihtd.addFamily(hcd3);
    List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
    indices.add(iSpec);
    indices.add(iSpec1);
    admin.createTable(ihtd);
    admin.addIndexes(TableName.valueOf(userTableName), indices);
    HTable table = new HTable(admin.getConfiguration(), userTableName);

    // test put with the indexed column
    Put p1 = new Put("row1".getBytes());
    p1.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p1.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p1.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p1);

    Put p2 = new Put("row2".getBytes());
    p2.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p2.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    p2.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p2);

    Put p3 = new Put("row3".getBytes());
    p3.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p3.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p3.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p3);

    Put p4 = new Put("row4".getBytes());
    p4.add("col1".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p4.add("col3".getBytes(), "ql".getBytes(), "dog".getBytes());
    table.put(p4);

    Put p5 = new Put("row5".getBytes());
    p5.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p5.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p5.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p5);

    Put p6 = new Put("row6".getBytes());
    p6.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col2".getBytes(), "ql".getBytes(), "cat".getBytes());
    p6.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p6);

    Put p7 = new Put("row7".getBytes());
    p7.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p7.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p7.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p7);

    Put p8 = new Put("row8".getBytes());
    p8.add("col1".getBytes(), "ql".getBytes(), "cat".getBytes());
    p8.add("col2".getBytes(), "ql".getBytes(), "dog".getBytes());
    p8.add("col3".getBytes(), "ql".getBytes(), "cat".getBytes());
    table.put(p8);

    Scan s = new Scan();
    FilterList filterList = new FilterList();
    // check for combination of cat in q1 and dog in q1
    SingleColumnValueFilter filter1 =
        new SingleColumnValueFilter("col1".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "cat".getBytes());
    filter1.setFilterIfMissing(true);
    SingleColumnValueFilter filter2 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter2.setFilterIfMissing(true);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    s.setFilter(filterList);
    ResultScanner scanner = table.getScanner(s);
    int i = 0;
    for (Result result : scanner) {
      i++;
    }
    Assert.assertEquals(
      "Should match for 5 rows in multiple index with diff column family successfully ", 5, i);

    admin.dropIndex(TableName.valueOf(userTableName),"Index2");
    Scan s1 = new Scan();
    FilterList filterList1 = new FilterList();

    SingleColumnValueFilter filter3 =
        new SingleColumnValueFilter("col2".getBytes(), "ql".getBytes(), CompareOp.EQUAL,
            "dog".getBytes());
    filter3.setFilterIfMissing(true);
    filterList1.addFilter(filter3);
    s1.setFilter(filterList1);
    ResultScanner scanner1 = table.getScanner(s1);
    int i1 = 0;
    for (Result result : scanner1) {
      i1++;
    }
    Assert.assertEquals(
      "Should match for 6 rows in multiple index with diff column family successfully ", 6, i1);

  }

  @Test(timeout = 180000)
  public void testIfIndexNameDoesNotExistShudThrowExceptionDuringDropIndex() throws Exception {
    String userTableName = "testDropIndexWithWrongIndexName";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    admin.createTable(ihtd);
    admin.addIndex(TableName.valueOf(userTableName), iSpec);
    try {
      admin.dropIndex(TableName.valueOf(userTableName),"UnknownIndex");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
    admin.dropIndex(TableName.valueOf(userTableName), iSpec.getName());
    try {
      admin.dropIndex(TableName.valueOf(userTableName),iSpec.getName());
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
    
  }

  @Test(timeout = 180000)
  public void testIfOneIndexExistAndItIsDroppedCorresponingIndexTableShouldBeDroppedForThat()
      throws Exception {
    String userTableName = "testDropIndexWith1Index";
    HTableDescriptor ihtd = new HTableDescriptor(TableName.valueOf(userTableName));
    HColumnDescriptor hcd = new HColumnDescriptor("col1");
    IndexSpecification iSpec = new IndexSpecification("Index1");
    iSpec.addIndexColumn(hcd, "ql", ValueType.String, 10);
    ihtd.addFamily(hcd);
    admin.createTable(ihtd);
    HTable table = new HTable(admin.getConfiguration(), userTableName);

    String row = "row1";
    Put p = new Put(row.getBytes());
    String val = "Val1";
    p.add("col1".getBytes(), "ql".getBytes(), val.getBytes());
    table.put(p);
    admin.addIndex(TableName.valueOf(userTableName),iSpec);
    admin.dropIndex(TableName.valueOf(userTableName),"Index1");
    Assert.assertFalse(admin.tableExists(userTableName + "_idx"));
  }
  
  @Test
  public void testShouldThrowIOExceptionIfPassedTableNameToDropIndexIsWrong() throws Exception {
    try {
      admin.dropIndex(TableName.valueOf("unknowntable"), "index");
    } catch (TableNotFoundException e) {
    }
  }
  
  
  public static class MockedRegionObserver extends IndexRegionObserver {
    
    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
        RegionScanner s) throws IOException {
      HTableDescriptor tableDesc = e.getEnvironment().getRegion().getTableDesc();
      if(tableDesc.getNameAsString().equals("testAddAndDropIndexesWithData")){
        if(count.compareAndSet(0, 1)) {
          try {
            admin
            .split(tableDesc.getName(), Bytes.toBytes("row23"));
            
            while (MetaReader.getRegionCount(UTIL.getConfiguration(), tableDesc.getNameAsString()) != 11) {
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 11) {
              Thread.sleep(100);
            }
            admin.getConnection().clearRegionCache(tableDesc.getTableName());
            admin.split(tableDesc.getName(),Bytes.toBytes("row25"));
            while(MetaReader.getRegionCount(new Configuration(UTIL.getConfiguration()), tableDesc.getNameAsString())!=12){
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 12) {
              Thread.sleep(100);
            }
            admin.getConnection().clearRegionCache(tableDesc.getTableName());
            admin.split(tableDesc.getName(),Bytes.toBytes("row27"));
            while(MetaReader.getRegionCount(new Configuration(UTIL.getConfiguration()), tableDesc.getNameAsString())!=13){
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 13) {
              Thread.sleep(100);
            }
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        } else if(count.compareAndSet(2, 3)) {
          try {
            admin
            .split(tableDesc.getName(), Bytes.toBytes("row43"));
            
            while (MetaReader.getRegionCount(UTIL.getConfiguration(), tableDesc.getNameAsString()) != 14) {
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 14) {
              Thread.sleep(100);
            }
            admin.getConnection().clearRegionCache(tableDesc.getTableName());
            admin.split(tableDesc.getName(),Bytes.toBytes("row45"));
            while(MetaReader.getRegionCount(new Configuration(UTIL.getConfiguration()), tableDesc.getNameAsString())!=15){
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 15) {
              Thread.sleep(100);
            }
            admin.getConnection().clearRegionCache(tableDesc.getTableName());
            admin.split(tableDesc.getName(),Bytes.toBytes("row47"));
            while(MetaReader.getRegionCount(new Configuration(UTIL.getConfiguration()), tableDesc.getNameAsString())!=16){
              Thread.sleep(100);
            }
            while (UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
                .getRegionsOfTable(tableDesc.getTableName()).size() != 16) {
              Thread.sleep(100);
            }
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }
      }
      return super.preScannerOpen(e, scan, s);
    }

  }
}

