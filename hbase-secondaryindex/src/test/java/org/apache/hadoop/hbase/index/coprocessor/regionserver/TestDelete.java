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

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestDelete {
  private static final String CF_EMP = "emp";
  private static final String CF_DEPT = "dept";
  private static final String CQ_ENAME = "ename";
  private static final String CQ_SAL = "salary";
  private static final String CQ_DNO = "dno";
  private static final String CQ_DNAME = "dname";
  private static final String START_KEY = "100";
  private static final String END_KEY = "200";

  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private String DIR = TEST_UTIL.getDataTestDir("TestHRegion").toString();

  private Path basedir;
  private HRegion userRegion;
  private HRegion indexRegion;
  private Map<String, IndexSpecification> indexMap;
  private IndexRegionObserver indexer;
  private Collection<String> indexPuts;
  private Collection<String> indexDeletes;

  @Before
  public void setup() throws Exception {
    prepare();

    indexMap = new HashMap<String, IndexSpecification>();
    index("idx_ename", CF_EMP, CQ_ENAME, ValueType.String, 10);
    index("idx_sal", CF_EMP, CQ_SAL, ValueType.String, 10);
    index("idx_dname", CF_DEPT, CQ_DNAME, ValueType.String, 10);
    index("idx_dno_ename", CF_DEPT, CQ_DNO, ValueType.String, 10);
    index("idx_dno_ename", CF_EMP, CQ_ENAME, ValueType.String, 10);
    indexer = new IndexRegionObserver();

    indexPuts = new TreeSet<String>();
    indexDeletes = new TreeSet<String>();
  }

  @After
  public void teardown() throws IOException {
    // Pass null table directory path to delete region.
    HFileArchiver.archiveRegion(basedir.getFileSystem(TEST_UTIL.getConfiguration()), basedir, null,
      new Path(FSUtils.getTableDir(basedir, userRegion.getRegionInfo().getTable()), userRegion
          .getRegionInfo().getEncodedName()));
  }

  @Test
  public void testDeleteVersion() throws IOException {
    // prepare test data
    put(101, 1, 1230);
    put(101, 1, 1240);

    // Delete version. Boundary scenario
    deleteVersion(101, CF_EMP, CQ_SAL, 1230);

    // should delete only one cell - one version
    assertTrue("Should delete exactly one nearest version of index entry (salary)",
      indexDeletes.size() == 1);
    // verify deletes against puts
    assertTrue("Index-deletes should be a subset of index puts",
      indexPuts.containsAll(indexDeletes));
  }

  @Test
  public void testDeleteCells() throws IOException {
    // prepare test data
    put(101, 0, 1230);
    put(101, 0, 1240);
    put(102, 2, 1240);

    // Delete cell - all versions of a qualifier
    deleteColumn(101, CF_EMP, CQ_SAL);

    assertTrue("Should delete all versions of a cell index entry (salary)",
      indexDeletes.size() == 2);
    // verify deletes against puts
    assertTrue("Index-deletes should be a subset of index puts",
      indexPuts.containsAll(indexDeletes));
  }

  @Test
  public void testDeleteFamily() throws IOException {
    // prepare test data
    put(101, 1, 1230);
    put(101, 2, 1240);
    put(102, 1, 1230);
    put(102, 0, 1240);

    // Delete family - All cells of the family
    deleteFamily(101, CF_EMP);

    // verify deletes against puts
    assertTrue("Index-deletes should be a subset of index puts",
      indexPuts.size() > indexDeletes.size());
    assertTrue("Index-deletes should be a subset of index puts",
      indexPuts.containsAll(indexDeletes));
  }

  @Test
  public void testDeleteRow() throws IOException {
    // prepare test data
    put(101, 1, 1230);
    put(101, 2, 1240);
    put(102, 1, 1250);

    // Delete row - all families & all cells
    deleteRow(101);
    deleteRow(102);

    // verify deletes against puts
    assertEquals("Puts and deletes are not same", indexPuts, indexDeletes);
  }

  private void prepare() throws IOException {
    basedir = new Path(DIR + "TestIndexDelete");
    Configuration conf = TEST_UTIL.getConfiguration();

    // Prepare the 'employee' table region
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("employee"));
    desc.addFamily(new HColumnDescriptor(CF_EMP).setMaxVersions(3));
    desc.addFamily(new HColumnDescriptor(CF_DEPT).setMaxVersions(3));
    HRegionInfo info =
        new HRegionInfo(desc.getTableName(), START_KEY.getBytes(), END_KEY.getBytes(), false);
    userRegion = HRegion.createHRegion(info, basedir, conf, desc);

    // Prepare the 'employee_idx' index table region
    HTableDescriptor idxDesc = new HTableDescriptor(TableName.valueOf("employee_idx"));
    idxDesc.addFamily(new HColumnDescriptor(Constants.IDX_COL_FAMILY));
    HRegionInfo idxInfo =
        new HRegionInfo(idxDesc.getTableName(), START_KEY.getBytes(), END_KEY.getBytes(), false);
    indexRegion = HRegion.createHRegion(idxInfo, basedir, conf, idxDesc);
  }

  private void index(String name, String cf, String cq, ValueType type, int maxSize) {
    IndexSpecification index = indexMap.get(name);
    if (index == null) {
      index = new IndexSpecification(name);
    }
    index.addIndexColumn(new HColumnDescriptor(cf), cq, type, maxSize);
    indexMap.put(name, index);
  }

  // For simplicity try to derive all details from eno and dno
  // Don't add department details when dno is 0 - Equivalent to adding EMP column family only
  private void put(int eno, int dno, long ts) throws IOException {
    Put put = new Put(toBytes("" + eno));

    put.add(toBytes(CF_EMP), toBytes(CQ_ENAME), ts, toBytes("emp_" + eno));
    put.add(toBytes(CF_EMP), toBytes(CQ_SAL), ts, toBytes("" + eno * 100));
    // Don't add department details when dno is 0
    // Equivalent to adding EMP column family only
    if (dno != 0) {
      put.add(toBytes(CF_DEPT), toBytes(CQ_DNO), ts, toBytes("" + dno));
      put.add(toBytes(CF_DEPT), toBytes(CQ_DNAME), ts, toBytes("dept_" + dno));
    }
    put.setDurability(Durability.SKIP_WAL);
    userRegion.put(put);
    for (IndexSpecification spec : indexMap.values()) {
      Put idxPut = IndexUtils.prepareIndexPut(put, spec, indexRegion);
      if (idxPut != null) {
        Cell kv = idxPut.get(Constants.IDX_COL_FAMILY, new byte[0]).get(0);
        indexPuts.add(Bytes.toString(idxPut.getRow()) + "_" + kv.getTimestamp());
      }
    }
  }

  private void deleteRow(int eno) throws IOException {
    Delete delete = new Delete(toBytes("" + eno));

    // Make the delete ready-to-eat by indexer
    for (byte[] family : userRegion.getTableDesc().getFamiliesKeys()) {
      delete.deleteFamily(family, delete.getTimeStamp());
    }
    delete(delete);
  }

  private void deleteFamily(int eno, String cf) throws IOException {
    Delete delete = new Delete(toBytes("" + eno));
    delete.deleteFamily(toBytes(cf));
    delete(delete);
  }

  private void deleteColumn(int eno, String cf, String cq) throws IOException {
    Delete delete = new Delete(toBytes("" + eno));
    delete.deleteColumns(toBytes(cf), toBytes(cq));
    delete(delete);
  }

  private void deleteVersion(int eno, String cf, String cq, long ts) throws IOException {
    Delete delete = new Delete(toBytes("" + eno));
    delete.deleteColumn(toBytes(cf), toBytes(cq), ts);
    delete(delete);
  }

  private void delete(Delete delete) throws IOException {
    Collection<? extends Mutation> deletes =
        indexer.prepareIndexDeletes(delete, userRegion,
          new ArrayList<IndexSpecification>(indexMap.values()), indexRegion);
    for (Mutation idxdelete : deletes) {
      Cell kv = idxdelete.getFamilyCellMap().get(Constants.IDX_COL_FAMILY).get(0);
      indexDeletes.add(Bytes.toString(idxdelete.getRow()) + "_" + kv.getTimestamp());
    }
  }
}
