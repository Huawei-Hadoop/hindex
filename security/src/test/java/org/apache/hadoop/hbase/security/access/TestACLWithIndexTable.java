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
package org.apache.hadoop.hbase.security.access;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestACLWithIndexTable {

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  // user with all permissions
  private static User SUPERUSER;
  private static User TEST_USER;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SecureTestUtil.enableSecurity(conf);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName() + ","
        + IndexMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AccessController.class.getName() + ","
        + SecureBulkLoadEndpoint.class.getName() + "," + IndexRegionObserver.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, IndexWALObserver.class.getName());
    conf.setInt("hbase.regionserver.lease.period", 10 * 60 * 1000);
    conf.setInt("hbase.rpc.timeout", 10 * 60 * 1000);
    conf.setBoolean("hbase.use.secondary.index", true);
    TEST_UTIL.startMiniCluster(2);

    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 5000);
    // create a set of test users
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    TEST_USER = User.createUserForTesting(conf, "testUser", new String[0]);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 180000)
  public void testCreateTable() throws Exception {
    // initilize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testCreateTable"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testCreateTable"), null, Permission.Action.ADMIN));

    PrivilegedExceptionAction createTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testCreateTable");
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        htd.addFamily(hcd);
        IndexSpecification iSpec = new IndexSpecification("spec");
        iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
        htd.addIndex(iSpec);
        admin.createTable(htd);
        return null;
      }
    };

    PrivilegedExceptionAction createIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testCreateTable_idx");
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        htd.addFamily(hcd);
        IndexSpecification iSpec = new IndexSpecification("spec");
        iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
        htd.addIndex(iSpec);
        admin.createTable(htd);
        return null;
      }
    };

    try {
      TEST_USER.runAs(createIndexTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(createTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      SUPERUSER.runAs(createTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception.Super user should be allowed to create tables");
    }

  }

  @Test(timeout = 180000)
  public void testDisableEnableTable() throws Exception {
    // initilize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class,
          Bytes.toBytes("testDisableEnableTable"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testDisableEnableTable"), null, Permission.Action.ADMIN));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testDisableEnableTable");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    // Creating the operation to be performed by the user
    PrivilegedExceptionAction disableTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(Bytes.toBytes("testDisableEnableTable"));
        return null;
      }
    };

    PrivilegedExceptionAction disableIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable("testDisableEnableTable_idx");
        return null;
      }
    };
    PrivilegedExceptionAction enableTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.enableTable(Bytes.toBytes("testDisableEnableTable"));
        return null;
      }
    };
    PrivilegedExceptionAction enableIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.enableTable(Bytes.toBytes("testDisableEnableTable_idx"));
        return null;
      }
    };

    // Execute all operations one by one
    try {
      TEST_USER.runAs(disableIndexTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }
    Assert.assertTrue("Index table should be enabled",
      admin.isTableEnabled("testDisableEnableTable_idx"));

    try {
      TEST_USER.runAs(disableTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
    while (!admin.isTableDisabled("testDisableEnableTable")) {
      Thread.sleep(10);
    }
    while (!admin.isTableDisabled("testDisableEnableTable_idx")) {
      Thread.sleep(10);
    }
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));
    Assert.assertTrue("Main table should be disabled",
      admin.isTableDisabled("testDisableEnableTable"));
    Assert.assertTrue("Index table should be disabled",
      admin.isTableDisabled("testDisableEnableTable_idx"));

    try {
      TEST_USER.runAs(enableIndexTable);
      Assert.fail("Index table should not get enabled. It should throw exception");
    } catch (Exception e) {

    }
    Assert.assertTrue("Index table should be disabled",
      admin.isTableDisabled("testDisableEnableTable_idx"));
    try {
      TEST_USER.runAs(enableTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception.");
    }
    while (!admin.isTableEnabled("testDisableEnableTable")) {
      Thread.sleep(10);
    }
    while (!admin.isTableEnabled("testDisableEnableTable_idx")) {
      Thread.sleep(10);
    }
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    Assert.assertTrue("Main table should be enabled",
      admin.isTableEnabled("testDisableEnableTable"));
    Assert.assertTrue("Index table should be enabled",
      admin.isTableEnabled("testDisableEnableTable_idx"));

  }

  @Test(timeout = 180000)
  public void testScanOperation() throws Exception {
    // initilize access control
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testScanOperation"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testScanOperation"), null, Permission.Action.READ));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testScanOperation");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    HTable table = new HTable(conf, "testScanOperation");
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("Test_val"));
      table.put(p);
    }

    PrivilegedExceptionAction scanTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HTable table = new HTable(conf, "testScanOperation");
        SingleColumnValueFilter svf =
            new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("q"), CompareOp.EQUAL,
                Bytes.toBytes("Test_val"));
        Scan s = new Scan();
        s.setFilter(svf);
        ResultScanner scanner = null;
        try {
          scanner = table.getScanner(s);
          Result result = scanner.next();
          while (result != null) {
            result = scanner.next();
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
        }
        return null;
      }
    };

    PrivilegedExceptionAction scanIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HTable table = new HTable(conf, "testScanOperation_idx");
        Scan s = new Scan();
        ResultScanner scanner = null;
        try {
          scanner = table.getScanner(s);
          Result result = scanner.next();
          while (result != null) {
            result = scanner.next();
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
        }
        return null;
      }
    };

    try {
      TEST_USER.runAs(scanIndexTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(scanTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception.");
    }
  }

  @Test(timeout = 180000)
  public void testCheckAndPut() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testCheckAndPut"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testCheckAndPut"), null, Permission.Action.WRITE, Permission.Action.READ));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testCheckAndPut");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    final IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    HTable table = new HTable(conf, "testCheckAndPut");
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("Test_val"));
      table.put(p);
    }

    HTable indexTable = new HTable(conf, "testCheckAndPut_idx");
    p = new Put(Bytes.toBytes("row" + 0));
    p.add(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, Bytes.toBytes("old_val"));
    indexTable.put(p);

    PrivilegedExceptionAction putInIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HTable indexTable2 = new HTable(conf, "testCheckAndPut_idx");
        Put put = new Put(Bytes.toBytes("row" + 0));
        put.add(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, Bytes.toBytes("latest"));
        indexTable2.checkAndPut(Bytes.toBytes("row" + 0), Constants.IDX_COL_FAMILY,
          Constants.IDX_COL_QUAL, Bytes.toBytes("old_val"), put);
        return null;
      }
    };

    PrivilegedExceptionAction putInMainTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HTable table2 = new HTable(conf, "testCheckAndPut");
        Put put = new Put(Bytes.toBytes("row" + 0));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("latest"));
        table2.checkAndPut(Bytes.toBytes("row" + 0), Bytes.toBytes("cf"), Bytes.toBytes("q"),
          Bytes.toBytes("Test_val"), put);
        return null;
      }
    };

    try {
      TEST_USER.runAs(putInIndexTable);
      Assert.fail("Should throw exception.");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(putInMainTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception.");
    }
  }

  @Test(timeout = 180000)
  public void testMoveRegionOp() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testMoveRegionOp"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testMoveRegionOp"), null, Permission.Action.ADMIN));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testMoveRegionOp");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    PrivilegedExceptionAction moveMainTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<HRegionInfo> tableRegions = admin.getTableRegions(Bytes.toBytes("testMoveRegionOp"));
        if (tableRegions.size() > 0) {
          HRegionInfo regionMoved = tableRegions.get(0);
          admin.move(regionMoved.getEncodedNameAsBytes(), null);
        }
        return null;
      }
    };

    PrivilegedExceptionAction moveIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<HRegionInfo> tableRegions =
            admin.getTableRegions(Bytes.toBytes("testMoveRegionOp_idx"));
        if (tableRegions.size() > 0) {
          HRegionInfo regionMoved = tableRegions.get(0);
          admin.move(regionMoved.getEncodedNameAsBytes(), null);
        }
        return null;
      }
    };

    try {
      TEST_USER.runAs(moveIndexTable);
      Assert.fail("Should throw exception.");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(moveMainTable);
    } catch (Exception e) {
      Assert.fail("should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testUnassignOperation() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class,
          Bytes.toBytes("testUnassignOperation"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testUnassignOperation"), null, Permission.Action.ADMIN));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testUnassignOperation");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    PrivilegedExceptionAction uassignMainRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<HRegionInfo> tableRegions =
            admin.getTableRegions(Bytes.toBytes("testUnassignOperation"));
        if (tableRegions.size() > 0) {
          HRegionInfo regionToBeUassigned = tableRegions.get(0);
          admin.unassign(regionToBeUassigned.getRegionName(), false);
        }
        return null;
      }
    };

    PrivilegedExceptionAction uassignIndexRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<HRegionInfo> tableRegions =
            admin.getTableRegions(Bytes.toBytes("testUnassignOperation_idx"));
        if (tableRegions.size() > 0) {
          HRegionInfo regionToBeUassigned = tableRegions.get(0);
          admin.unassign(regionToBeUassigned.getRegionName(), false);
        }
        return null;
      }
    };

    try {
      TEST_USER.runAs(uassignIndexRegion);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(uassignMainRegion);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testSplitOp() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testSplitOp"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testSplitOp"), null, Permission.Action.ADMIN));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testSplitOp");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    HTable table = new HTable(conf, "testSplitOp");
    Put p = null;
    for (int i = 0; i < 10; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("test_val"));
      table.put(p);
    }

    PrivilegedExceptionAction splitIndexRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.split("testSplitOp_idx");
        return null;
      }
    };

    PrivilegedExceptionAction splitMainRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.split("testSplitOp");
        return null;
      }
    };

    try {
      TEST_USER.runAs(splitIndexRegion);
      Assert.fail("Should throw exception.");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(splitMainRegion);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testCompactionOp() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testCompactionOp"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testCompactionOp"), null, Permission.Action.ADMIN));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testCompactionOp");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    HTable table = new HTable(conf, "testCompactionOp");
    Put p = null;
    for (int i = 0; i < 100; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("test_val"));
      table.put(p);
      if (i % 10 == 0) {
        admin.flush("testCompactionOp");
        admin.flush("testCompactionOp_idx");
      }
    }

    PrivilegedExceptionAction compactIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.compact("testCompactionOp_idx");
        return null;
      }
    };

    PrivilegedExceptionAction compactMainTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.compact("testCompactionOp");
        return null;
      }
    };

    try {
      TEST_USER.runAs(compactIndexTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {
    }

    try {
      TEST_USER.runAs(compactMainTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testDeleteTable() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testDeleteTable"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testDeleteTable"), null, Permission.Action.ADMIN, Permission.Action.CREATE));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testDeleteTable");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    admin.disableTable("testDeleteTable");
    while (!admin.isTableDisabled("testDeleteTable")) {
      Thread.sleep(10);
    }
    while (!admin.isTableDisabled("testDeleteTable_idx")) {
      Thread.sleep(10);
    }
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    PrivilegedExceptionAction deleteIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin2 = new HBaseAdmin(conf);
        admin2.deleteTable(Bytes.toBytes("testDeleteTable_idx"));
        return null;
      }
    };

    PrivilegedExceptionAction deleteMainTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin2 = new HBaseAdmin(conf);
        admin2.deleteTable(Bytes.toBytes("testDeleteTable"));
        return null;
      }
    };

    try {
      TEST_USER.runAs(deleteIndexTable);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(deleteMainTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testflushOp() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testflushOp"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testflushOp"), null, Permission.Action.ADMIN, Permission.Action.CREATE));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testflushOp");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));

    HTable table = new HTable(conf, "testflushOp");
    Put p = null;
    for (int i = 0; i < 100; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"), Bytes.toBytes("test_val"));
      table.put(p);
    }

    PrivilegedExceptionAction flushIndexRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.flush("testflushOp_idx");
        return null;
      }
    };

    PrivilegedExceptionAction flushMainRegion = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.flush("testflushOp");
        return null;
      }
    };

    try {
      TEST_USER.runAs(flushIndexRegion);
      Assert.fail("Should throw exception.");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(flushMainRegion);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception.");
    }
  }

  @Test(timeout = 180000)
  public void testModifyTable() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testModifyTable"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testModifyTable"), null, Permission.Action.ADMIN, Permission.Action.CREATE));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testModifyTable");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));
    admin.disableTable("testModifyTable");

    PrivilegedExceptionAction modifyIndexTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor("testModifyTable_idx");
        htd.addFamily(new HColumnDescriptor("d"));
        htd.addFamily(new HColumnDescriptor("d1"));
        admin.modifyTable(Bytes.toBytes("testModifyTable_idx"), htd);
        return null;
      }
    };

    PrivilegedExceptionAction modifyMainTable = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        IndexedHTableDescriptor ihtd = new IndexedHTableDescriptor("testModifyTable");
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        HColumnDescriptor hcd1 = new HColumnDescriptor("cf1");
        ihtd.addFamily(hcd);
        ihtd.addFamily(hcd1);
        IndexSpecification iSpec = new IndexSpecification("spec");
        IndexSpecification iSpec1 = new IndexSpecification("spec1");
        iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
        iSpec1.addIndexColumn(hcd1, "q", ValueType.String, 10);
        ihtd.addIndex(iSpec);
        ihtd.addIndex(iSpec1);
        admin.modifyTable(Bytes.toBytes("testModifyTable"), ihtd);
        return null;
      }
    };

    try {
      TEST_USER.runAs(modifyIndexTable);
      Assert.fail("Should throw exception.");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(modifyMainTable);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

  @Test(timeout = 180000)
  public void testModifyFamily() throws Exception {
    HTable meta = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    AccessControllerProtocol protocol =
        meta.coprocessorProxy(AccessControllerProtocol.class, Bytes.toBytes("testModifyFamily"));
    protocol.grant(new UserPermission(Bytes.toBytes(TEST_USER.getShortName()), Bytes
        .toBytes("testModifyFamily"), null, Permission.Action.ADMIN, Permission.Action.CREATE));

    HBaseAdmin admin = new HBaseAdmin(conf);
    IndexedHTableDescriptor htd = new IndexedHTableDescriptor("testModifyFamily");
    HColumnDescriptor hcd = new HColumnDescriptor("cf");
    htd.addFamily(hcd);
    IndexSpecification iSpec = new IndexSpecification("spec");
    iSpec.addIndexColumn(hcd, "q", ValueType.String, 10);
    htd.addIndex(iSpec);
    admin.createTable(htd);
    ZKAssign.blockUntilNoRIT(HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL));
    admin.disableTable("testModifyFamily");

    PrivilegedExceptionAction modifyIndexFamily = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HColumnDescriptor hcd = new HColumnDescriptor("d");
        hcd.setMaxVersions(4);
        admin.modifyColumn("testModifyFamily_idx", hcd);
        return null;
      }
    };

    PrivilegedExceptionAction modifyMainFamily = new PrivilegedExceptionAction() {
      @Override
      public Object run() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HColumnDescriptor hcd = new HColumnDescriptor("cf");
        hcd.setMaxVersions(4);
        admin.modifyColumn("testModifyFamily", hcd);
        return null;
      }
    };

    try {
      TEST_USER.runAs(modifyIndexFamily);
      Assert.fail("Should throw exception");
    } catch (Exception e) {

    }

    try {
      TEST_USER.runAs(modifyMainFamily);
    } catch (Exception e) {
      Assert.fail("Should not throw any exception");
    }
  }

}
