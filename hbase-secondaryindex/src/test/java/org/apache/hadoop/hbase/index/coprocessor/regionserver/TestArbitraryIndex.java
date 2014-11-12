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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestArbitraryIndex {

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setBoolean("hbase.use.secondary.index", true);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SecIndexLoadBalancer.class,
        LoadBalancer.class);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIndex() throws Exception {
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    TableName tableName = TableName.valueOf("table1");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    // cf1 and cf2 are indexing but not cf3
    byte[] cf1 = Bytes.toBytes("cf1");
    byte[] cf2 = Bytes.toBytes("cf2");
    byte[] cf3 = Bytes.toBytes("cf3");
    HColumnDescriptor hcd1 = new HColumnDescriptor(cf1);
    htd.addFamily(hcd1);
    HColumnDescriptor hcd2 = new HColumnDescriptor(cf2);
    htd.addFamily(hcd2);
    HColumnDescriptor hcd3 = new HColumnDescriptor(cf3);
    htd.addFamily(hcd3);
    TableIndices indices = new TableIndices();
    indices.addIndex(IndexSpecification.createArbitraryColumnIndex(hcd1, hcd2));
    htd.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    try {
      admin.createTable(htd);
    } finally {
      admin.close();
    }

    HTable ht = new HTable(UTIL.getConfiguration(), tableName);

    Put p = new Put("r1".getBytes());
    p.add(cf1, "q1".getBytes(), "v1".getBytes());
    p.add(cf1, "q2".getBytes(), "v2".getBytes());
    p.add(cf2, "q3".getBytes(), "v3".getBytes());
    p.add(cf3, "q4".getBytes(), "v4".getBytes());

    ht.put(p);

    p = new Put("r2".getBytes());
    p.add(cf1, "q1".getBytes(), "v1".getBytes());
    p.add(cf1, "q2".getBytes(), "v2".getBytes());
    p.add(cf2, "q3".getBytes(), "v23".getBytes());
    p.add(cf3, "q4".getBytes(), "v4".getBytes());

    ht.put(p);

    p = new Put("r3".getBytes());
    p.add(cf1, "q1".getBytes(), "v11".getBytes());
    p.add(cf1, "q2".getBytes(), "v2".getBytes());
    p.add(cf2, "q3".getBytes(), "v23".getBytes());
    p.add(cf3, "q4".getBytes(), "v4".getBytes());

    ht.put(p);

    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    HTable indexTable = new HTable(UTIL.getConfiguration(), indexTableName);
    ResultScanner scanner = indexTable.getScanner(new Scan());
    Result result = null;
    int count = 0;
    while ((result = scanner.next()) != null) {
      System.out.println(result);
      count++;
    }
    assertEquals(9, count);

    System.out.println("============================================");
    Scan s = new Scan();
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
    fl.addFilter(new SingleColumnValueFilter(cf1, "q1".getBytes(), CompareOp.EQUAL, "v1".getBytes()));
    fl.addFilter(new SingleColumnValueFilter(cf2, "q3".getBytes(), CompareOp.EQUAL, "v23"
        .getBytes()));
    s.setFilter(fl);
    scanner = ht.getScanner(s);
    List<String> rows = new ArrayList<String>();
    while ((result = scanner.next()) != null) {
      System.out.println(result);
      rows.add(Bytes.toString(result.getRow()));
    }
    assertEquals(1, rows.size());
    assertTrue(rows.contains("r2"));
    rows.clear();
    System.out.println("============================================");
    System.out.println("****************  OR  **********************");
    s = new Scan();
    fl = new FilterList(Operator.MUST_PASS_ONE);
    fl.addFilter(new SingleColumnValueFilter(cf1, "q1".getBytes(), CompareOp.EQUAL, "v11"
        .getBytes()));
    fl.addFilter(new SingleColumnValueFilter(cf2, "q3".getBytes(), CompareOp.EQUAL, "v23"
        .getBytes()));
    s.setFilter(fl);
    scanner = ht.getScanner(s);
    while ((result = scanner.next()) != null) {
      System.out.println(result);
      rows.add(Bytes.toString(result.getRow()));
    }
    assertEquals(2, rows.size());
    assertTrue(rows.contains("r2"));
    assertTrue(rows.contains("r3"));
  }
}
