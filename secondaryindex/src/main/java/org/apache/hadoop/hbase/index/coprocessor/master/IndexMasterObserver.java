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
package org.apache.hadoop.hbase.index.coprocessor.master;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserverExt;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKTable;

/**
 * Defines of coprocessor hooks(to support secondary indexing) of operations on
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 */

public class IndexMasterObserver extends BaseMasterObserver implements MasterObserverExt {

  private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class.getName());

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.coprocessor.BaseMasterObserver#preCreateTable(org
   * .apache.hadoop.hbase.coprocessor.ObserverContext, org.apache.hadoop.hbase.HTableDescriptor,
   * org.apache.hadoop.hbase.HRegionInfo[])
   */
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    LOG.info("Entered into preCreateTable.");
    MasterServices master = ctx.getEnvironment().getMasterServices();
    if (desc instanceof IndexedHTableDescriptor) {
      Map<Column, Pair<ValueType, Integer>> indexColDetails =
          new HashMap<Column, Pair<ValueType, Integer>>();
      String tableName = desc.getNameAsString();
      checkEndsWithIndexSuffix(tableName);
      String indexTableName = IndexUtils.getIndexTableName(tableName);
      List<IndexSpecification> indices = ((IndexedHTableDescriptor) desc).getIndices();
      // Even if indices list is empty,it will create index table also.
      if (indices.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Empty indices. Index table may not created"
              + " if master goes down in between user table creation");
        }
      }
      LOG.trace("Checking whether column families in "
          + "index specification are in actual table column familes.");
      for (IndexSpecification iSpec : indices) {
        checkColumnsForValidityAndConsistency(desc, iSpec, indexColDetails);
      }
      LOG.trace("Column families in index specifications " + "are in actual table column familes.");

      boolean isTableExists = MetaReader.tableExists(master.getCatalogTracker(), tableName);
      boolean isIndexTableExists =
          MetaReader.tableExists(master.getCatalogTracker(), indexTableName);

      if (isTableExists && isIndexTableExists) {
        throw new TableExistsException("Table " + tableName + " already exist.");
      } else if (isIndexTableExists) {
        disableAndDeleteTable(master, indexTableName);
      }
    }
    LOG.info("Exiting from preCreateTable.");
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HTableDescriptor htd) throws IOException {
    String table = Bytes.toString(tableName);
    MasterServices master = ctx.getEnvironment().getMasterServices();
    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations = null;
    LOG.info("Entering postModifyTable for the table " + table);
    if (htd instanceof IndexedHTableDescriptor) {
      TableDescriptors tableDescriptors = master.getTableDescriptors();
      Map<String, HTableDescriptor> allTableDesc = tableDescriptors.getAll();
      String indexTableName = IndexUtils.getIndexTableName(tableName);
      if (allTableDesc.containsKey(indexTableName)) {
        // Do table modification
        List<IndexSpecification> indices = ((IndexedHTableDescriptor) htd).getIndices();
        if (indices.isEmpty()) {
          LOG.error("Empty indices are passed to modify the table " + Bytes.toString(tableName));
          return;
        }
        IndexManager idxManager = IndexManager.getInstance();
        idxManager.removeIndices(table);
        idxManager.addIndexForTable(table, indices);
        LOG.info("Successfully updated the indexes for the table  " + table + " to " + indices);
      } else {
        try {
          tableRegionsAndLocations =
              MetaReader.getTableRegionsAndLocations(master.getCatalogTracker(), tableName, true);
        } catch (InterruptedException e) {
          LOG.error("Exception while trying to create index table for the existing table " + table);
          return;
        }
        if (tableRegionsAndLocations != null) {
          HRegionInfo[] regionInfo = new HRegionInfo[tableRegionsAndLocations.size()];
          for (int i = 0; i < tableRegionsAndLocations.size(); i++) {
            regionInfo[i] = tableRegionsAndLocations.get(i).getFirst();
          }

          byte[][] splitKeys = IndexUtils.getSplitKeys(regionInfo);
          IndexedHTableDescriptor iDesc = (IndexedHTableDescriptor) htd;
          createSecondaryIndexTable(iDesc, splitKeys, master, true);
        }
      }
    }
    LOG.info("Exiting postModifyTable for the table " + table);
  }

  private void checkColumnsForValidityAndConsistency(HTableDescriptor desc,
      IndexSpecification iSpec, Map<Column, Pair<ValueType, Integer>> indexColDetails)
      throws IOException {
    for (ColumnQualifier cq : iSpec.getIndexColumns()) {
      if (null == desc.getFamily(cq.getColumnFamily())) {
        String message =
            "Column family " + cq.getColumnFamilyString() + " in index specification "
                + iSpec.getName() + " not in Column families of table " + desc.getNameAsString()
                + '.';
        LOG.error(message);
        IllegalArgumentException ie = new IllegalArgumentException(message);
        throw new IOException(ie);
      }
      Column column = new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition());
      ValueType type = cq.getType();
      int maxlength = cq.getMaxValueLength();
      Pair<ValueType, Integer> colDetail = indexColDetails.get(column);
      if (null != colDetail) {
        if (!colDetail.getFirst().equals(type) || colDetail.getSecond() != maxlength) {
          throw new IOException("ValueType/max value length of column " + column
              + " not consistent across the indices");
        }
      } else {
        indexColDetails.put(column, new Pair<ValueType, Integer>(type, maxlength));
      }
    }
  }

  private void checkEndsWithIndexSuffix(String tableName) throws IOException {
    if (tableName.endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      String message =
          "User table name should not be ends with " + Constants.INDEX_TABLE_SUFFIX + '.';
      LOG.error(message);
      IllegalArgumentException ie = new IllegalArgumentException(message);
      throw new IOException(ie);
    }
  }

  private void disableAndDeleteTable(MasterServices master, String tableName) throws IOException {
    byte[] tableNameInBytes = Bytes.toBytes(tableName);
    LOG.error(tableName + " already exists.  Disabling and deleting table " + tableName + '.');
    boolean disabled = master.getAssignmentManager().getZKTable().isDisabledTable(tableName);
    if (false == disabled) {
      LOG.info("Disabling table " + tableName + '.');
      new DisableTableHandler(master, tableNameInBytes, master.getCatalogTracker(),
          master.getAssignmentManager(), false).process();
      if (false == master.getAssignmentManager().getZKTable().isDisabledTable(tableName)) {
        throw new IOException("Table " + tableName + " not disabled.");
      }
    }
    LOG.info("Disabled table " + tableName + '.');
    LOG.info("Deleting table " + tableName + '.');
    new DeleteTableHandler(tableNameInBytes, master, master).process();
    if (true == MetaReader.tableExists(master.getCatalogTracker(), tableName)) {
      throw new IOException("Table " + tableName + " not  deleted.");
    }
    LOG.info("Deleted table " + tableName + '.');
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    LOG.info("Entered into postCreateTableHandler of table " + desc.getNameAsString() + '.');
    if (desc instanceof IndexedHTableDescriptor) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      byte[][] splitKeys = IndexUtils.getSplitKeys(regions);
      // In case of post call for the index table creation, it wont be
      // IndexedHTableDescriptor
      IndexedHTableDescriptor iDesc = (IndexedHTableDescriptor) desc;
      createSecondaryIndexTable(iDesc, splitKeys, master, false);
      // if there is any user scenarios
      // we can add index datails to index manager
    }
    LOG.info("Exiting from postCreateTableHandler of table " + desc.getNameAsString() + '.');
  }

  /**
   * @param IndexedHTableDescriptor iDesc
   * @param HRegionInfo [] regions
   * @param MasterServices master
   * @throws NotAllMetaRegionsOnlineException
   * @throws IOException
   */
  private void createSecondaryIndexTable(IndexedHTableDescriptor iDesc, byte[][] splitKeys,
      MasterServices master, boolean disableTable) throws NotAllMetaRegionsOnlineException,
      IOException {
    String indexTableName = IndexUtils.getIndexTableName(iDesc.getNameAsString());
    LOG.info("Creating secondary index table " + indexTableName + " for table "
        + iDesc.getNameAsString() + '.');
    HTableDescriptor indexTableDesc = new HTableDescriptor(indexTableName);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(Constants.IDX_COL_FAMILY);
    String dataBlockEncodingAlgo =
        master.getConfiguration().get("index.data.block.encoding.algo", "NONE");
    DataBlockEncoding[] values = DataBlockEncoding.values();

    for (DataBlockEncoding dataBlockEncoding : values) {
      if (dataBlockEncoding.toString().equals(dataBlockEncodingAlgo)) {
        columnDescriptor.setDataBlockEncoding(dataBlockEncoding);
      }
    }

    indexTableDesc.addFamily(columnDescriptor);
    indexTableDesc.setValue(HTableDescriptor.SPLIT_POLICY,
      ConstantSizeRegionSplitPolicy.class.getName());
    indexTableDesc.setMaxFileSize(Long.MAX_VALUE);
    LOG.info("Setting the split policy for the Index Table " + indexTableName
        + " as ConstantSizeRegionSplitPolicy with maxFileSize as " + Long.MAX_VALUE + '.');
    HRegionInfo[] newRegions = getHRegionInfos(indexTableDesc, splitKeys);
    new CreateTableHandler(master, master.getMasterFileSystem(), master.getServerManager(),
        indexTableDesc, master.getConfiguration(), newRegions, master.getCatalogTracker(),
        master.getAssignmentManager()).process();
    // Disable the index table so that when we enable the main table both can be enabled
    if (disableTable) {
      new DisableTableHandler(master, Bytes.toBytes(indexTableName), master.getCatalogTracker(),
          master.getAssignmentManager(), false).process();
    }
    LOG.info("Created secondary index table " + indexTableName + " for table "
        + iDesc.getNameAsString() + '.');

  }

  private HRegionInfo[] getHRegionInfos(HTableDescriptor hTableDescriptor, byte[][] splitKeys) {
    HRegionInfo[] hRegionInfos = null;
    if (splitKeys == null || splitKeys.length == 0) {
      hRegionInfos = new HRegionInfo[] { new HRegionInfo(hTableDescriptor.getName(), null, null) };
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] = new HRegionInfo(hTableDescriptor.getName(), startKey, endKey);
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo hri)
      throws IOException {
    boolean isRegionInTransition = checkRegionInTransition(ctx, hri);
    if (isRegionInTransition) {
      LOG.info("Not calling assign for region " + hri.getRegionNameAsString()
          + "because the region is already in transition.");
      ctx.bypass();
      return;
    }
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
    LOG.info("Entering into postAssign of region " + regionInfo.getRegionNameAsString() + '.');

    if (false == regionInfo.getTableNameAsString().endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      AssignmentManager am = master.getAssignmentManager();
      // waiting until user region is removed from transition.
      long timeout =
          master.getConfiguration()
              .getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
      Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>(1);
      regionSet.add(regionInfo);
      try {
        am.waitUntilNoRegionsInTransition(timeout, regionSet);
        am.waitForAssignment(regionInfo, timeout);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while region in assignment.");
        }
      }
      ServerName sn = am.getRegionServerOfRegion(regionInfo);
      String indexTableName = IndexUtils.getIndexTableName(regionInfo.getTableNameAsString());
      List<HRegionInfo> tableRegions = am.getRegionsOfTable(Bytes.toBytes(indexTableName));
      for (HRegionInfo hRegionInfo : tableRegions) {
        if (0 == Bytes.compareTo(hRegionInfo.getStartKey(), regionInfo.getStartKey())) {
          am.addPlan(hRegionInfo.getEncodedName(), new RegionPlan(hRegionInfo, null, sn));
          LOG.info("Assigning region " + hRegionInfo.getRegionNameAsString() + " to server " + sn
              + '.');
          am.assign(hRegionInfo, true, false, false);
        }
      }
    }
    LOG.info("Exiting from postAssign " + regionInfo.getRegionNameAsString() + '.');
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo hri,
      boolean force) throws IOException {
    boolean isRegionInTransition = checkRegionInTransition(ctx, hri);
    if (isRegionInTransition) {
      LOG.info("Not calling move for region because region" + hri.getRegionNameAsString()
          + " is already in transition.");
      ctx.bypass();
      return;
    }
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo hri,
      ServerName srcServer, ServerName destServer) throws IOException {
    boolean isRegionInTransition = checkRegionInTransition(ctx, hri);
    if (isRegionInTransition) {
      LOG.info("Not calling move for region " + hri.getRegionNameAsString()
          + "because the region is already in transition.");
      ctx.bypass();
      return;
    }
  }

  // This is just an additional precaution. This cannot ensure 100% that the RIT regions
  // will not be picked up.
  // Because the RIT map that is taken here is the copy of original RIT map and there is
  // no sync mechanism also.
  private boolean checkRegionInTransition(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo hri) {
    MasterServices master = ctx.getEnvironment().getMasterServices();
    AssignmentManager am = master.getAssignmentManager();
    boolean isRegionInTransition = false;
    String tableName = hri.getTableNameAsString();
    if (false == IndexUtils.isIndexTable(tableName)) {
      NavigableMap<String, RegionState> regionsInTransition = am.getRegionsInTransition();
      RegionState regionState = regionsInTransition.get(hri.getEncodedName());
      if (regionState != null) {
        isRegionInTransition = true;
      } else {
        String indexTableName = IndexUtils.getIndexTableName(tableName);
        for (Entry<String, RegionState> region : regionsInTransition.entrySet()) {
          HRegionInfo regionInfo = region.getValue().getRegion();
          if (indexTableName.equals(regionInfo.getTableNameAsString())) {
            if (Bytes.compareTo(hri.getStartKey(), regionInfo.getStartKey()) == 0) {
              isRegionInTransition = true;
              break;
            }
          }
        }
      }
    }
    return isRegionInTransition;
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo,
      ServerName srcServer, ServerName destServer) throws IOException {
    LOG.info("Entering into postMove " + regionInfo.getRegionNameAsString() + '.');
    if (false == regionInfo.getTableNameAsString().endsWith(Constants.INDEX_TABLE_SUFFIX)) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      AssignmentManager am = master.getAssignmentManager();
      // waiting until user region is removed from transition.
      long timeout =
          master.getConfiguration()
              .getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
      Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>(1);
      regionSet.add(regionInfo);
      try {
        am.waitUntilNoRegionsInTransition(timeout, regionSet);
        am.waitForAssignment(regionInfo, timeout);
        destServer = am.getRegionServerOfRegion(regionInfo);
        am.putRegionPlan(regionInfo, destServer);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while region in assignment.");
        }
      }
      String indexTableName = IndexUtils.getIndexTableName(regionInfo.getTableNameAsString());
      List<HRegionInfo> tableRegions = am.getRegionsOfTable(Bytes.toBytes(indexTableName));
      for (HRegionInfo indexRegionInfo : tableRegions) {
        if (0 == Bytes.compareTo(indexRegionInfo.getStartKey(), regionInfo.getStartKey())) {
          LOG.info("Assigning region " + indexRegionInfo.getRegionNameAsString() + "from "
              + srcServer + " to server " + destServer + '.');
          am.putRegionPlan(indexRegionInfo, destServer);
          am.addPlan(indexRegionInfo.getEncodedName(), new RegionPlan(indexRegionInfo, null,
              destServer));
          am.unassign(indexRegionInfo);
          /*
           * ((HMaster) master).move(indexRegionInfo.getEncodedNameAsBytes(),
           * Bytes.toBytes(destServer.getServerName()));
           */
        }
      }
    }
    LOG.info("Exiting from postMove " + regionInfo.getRegionNameAsString() + '.');
  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
    LOG.info("Entered into postDisableTableHandler of table " + Bytes.toString(tableName));
    MasterServices master = ctx.getEnvironment().getMasterServices();
    AssignmentManager am = master.getAssignmentManager();
    try {
      if (false == IndexUtils.isIndexTable(Bytes.toString(tableName))) {
        String indexTableName = IndexUtils.getIndexTableName(tableName);
        // Index table may not present following three cases.
        // 1) Index details are not specified during table creation then index table wont be
        // created.
        // 2) Even we specify index details if master restarted in the middle of user table creation
        // corresponding index table wont be created. But without creating index table user table
        // wont
        // be disabled. No need to call disable for index table at that time.
        // 3) Index table may be deleted but this wont happen without deleting user table.
        if (true == am.getZKTable().isTablePresent(indexTableName)) {
          long timeout =
              master.getConfiguration().getLong("hbase.bulk.assignment.waiton.empty.rit",
                5 * 60 * 1000);
          // Both user table and index table should not be in enabling/disabling state at a time.
          // If disable is progress for user table then index table should be in ENABLED state.
          // If enable is progress for index table wait until table enabled.
          waitUntilTableEnabled(timeout, indexTableName, am.getZKTable());
          if (waitUntilTableEnabled(timeout, indexTableName, am.getZKTable())) {
            new DisableTableHandler(master, Bytes.toBytes(indexTableName),
                master.getCatalogTracker(), am, false).process();
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table " + indexTableName + " not in ENABLED state to disable.");
            }
          }
        }
      }
    } finally {
      // clear user table region plans in secondary index load balancer.
      clearRegionPlans((HMaster) master, Bytes.toString(tableName));
    }
    LOG.info("Exiting from postDisableTableHandler of table " + Bytes.toString(tableName));
  }

  private void clearRegionPlans(HMaster master, String tableName) {
    ((SecIndexLoadBalancer) master.getBalancer()).clearTableRegionPlans(tableName);
  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
    LOG.info("Entered into postEnableTableHandler of table " + Bytes.toString(tableName));
    if (false == IndexUtils.isIndexTable(Bytes.toString(tableName))) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      AssignmentManager am = master.getAssignmentManager();
      String indexTableName = IndexUtils.getIndexTableName(tableName);
      // Index table may not present in three cases
      // 1) Index details are not specified during table creation then index table wont be created.
      // 2) Even we specify index details if master restarted in the middle of user table creation
      // corresponding index table wont be created. Then no need to call enable for index table
      // because it will be created as part of preMasterInitialization and enable.
      // 3) Index table may be deleted but this wont happen without deleting user table.
      if (true == am.getZKTable().isTablePresent(indexTableName)) {
        long timeout =
            master.getConfiguration().getLong("hbase.bulk.assignment.waiton.empty.rit",
              5 * 60 * 1000);
        // Both user table and index table should not be in enabling/disabling state at a time.
        // If enable is progress for user table then index table should be in disabled state.
        // If disable is progress for index table wait until table disabled.
        if (waitUntilTableDisabled(timeout, indexTableName, am.getZKTable())) {
          new EnableTableHandler(master, Bytes.toBytes(indexTableName), master.getCatalogTracker(),
              am, false).process();
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Table " + indexTableName + " not in DISABLED state to enable.");
          }
        }
      }
    }
    LOG.info("Exiting from postEnableTableHandler of table " + Bytes.toString(tableName));

  }

  private boolean waitUntilTableDisabled(long timeout, String tableName, ZKTable zk) {
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    boolean disabled = false;
    while (!(disabled = zk.isDisabledTable(tableName)) && remaining > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while waiting for table" + tableName + " set to DISABLED.");
        }
      }
      remaining = timeout - (System.currentTimeMillis() - startTime);
    }
    if (remaining <= 0) {
      return disabled;
    } else {
      return true;
    }
  }

  private boolean waitUntilTableEnabled(long timeout, String tableName, ZKTable zk) {
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    boolean enabled = false;
    while (!(enabled = zk.isEnabledTable(tableName)) && remaining > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while waiting for table " + tableName + "state set to ENABLED.");
        }
      }
      remaining = timeout - (System.currentTimeMillis() - startTime);
    }
    if (remaining <= 0) {
      return enabled;
    } else {
      return true;
    }

  }

  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
    LOG.info("Entered into postDeleteTableHandler of table " + Bytes.toString(tableName) + '.');
    MasterServices master = ctx.getEnvironment().getMasterServices();
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    boolean indexTablePresent =
        master.getAssignmentManager().getZKTable().isTablePresent(indexTableName);
    // Not checking for disabled state because before deleting user table both user and index table
    // should be disabled.
    if ((false == IndexUtils.isIndexTable(Bytes.toString(tableName))) && indexTablePresent) {
      new DeleteTableHandler(Bytes.toBytes(indexTableName), master, master).process();
    }
    LOG.info("Exiting from postDeleteTableHandler of table " + Bytes.toString(tableName) + '.');
  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    LOG.info("Entering into preMasterInitialization.");
    MasterServices master = ctx.getEnvironment().getMasterServices();
    AssignmentManager am = master.getAssignmentManager();
    ZKTable zkTable = am.getZKTable();
    long timeout =
        master.getConfiguration().getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
    try {
      am.waitUntilNoRegionsInTransition(timeout);
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Interrupted while waiting for the regions in transition to complete.", e);
      }
    }

    TableDescriptors tableDescriptors = master.getTableDescriptors();
    Map<String, HTableDescriptor> descMap = tableDescriptors.getAll();
    Collection<HTableDescriptor> htds = descMap.values();
    IndexedHTableDescriptor iHtd = null;
    Configuration conf = master.getConfiguration();
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    Path rootPath = FSUtils.getRootDir(conf);
    for (HTableDescriptor htd : htds) {
      if (false == htd.getNameAsString().endsWith(Constants.INDEX_TABLE_SUFFIX)) {
        FSDataInputStream fsDataInputStream = null;
        try {
          Path path = FSUtils.getTablePath(rootPath, htd.getName());
          FileStatus status = getTableInfoPath(fs, path);
          if (null == status) {
            return;
          }
          fsDataInputStream = fs.open(status.getPath());
          iHtd = new IndexedHTableDescriptor();
          iHtd.readFields(fsDataInputStream);
        } catch (EOFException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(iHtd.getNameAsString() + " is normal table and not an indexed table.", e);
          }
        } catch (IOException i) {
          throw i;
        } finally {
          if (null != fsDataInputStream) {
            fsDataInputStream.close();
          }
        }
        if (false == iHtd.getIndices().isEmpty()) {
          String tableName = iHtd.getNameAsString();
          String indexTableName = IndexUtils.getIndexTableName(tableName);
          boolean tableExists = MetaReader.tableExists(master.getCatalogTracker(), tableName);
          boolean indexTableExists =
              MetaReader.tableExists(master.getCatalogTracker(), indexTableName);
          if ((true == tableExists) && (false == indexTableExists)) {
            LOG.info("Table has index specification details but " + "no corresponding index table.");
            List<HRegionInfo> regions =
                MetaReader.getTableRegions(master.getCatalogTracker(), iHtd.getName());
            HRegionInfo[] regionsArray = new HRegionInfo[regions.size()];
            byte[][] splitKeys = IndexUtils.getSplitKeys(regions.toArray(regionsArray));
            createSecondaryIndexTable(iHtd, splitKeys, master, false);
          } else if (true == tableExists && true == indexTableExists) {
            // If both tables are present both should be in same state in zookeeper. If tables are
            // partially enabled or disabled they will be processed as part of recovery
            // enabling/disabling tables.
            // if user table is in ENABLED state and index table is in DISABLED state means master
            // restarted as soon as user table enabled. So here we need to enable index table.
            if (zkTable.isEnabledTable(tableName) && zkTable.isDisabledTable(indexTableName)) {
              new EnableTableHandler(master, Bytes.toBytes(indexTableName),
                  master.getCatalogTracker(), am, false).process();
            } else if (zkTable.isDisabledTable(tableName) && zkTable.isEnabledTable(indexTableName)) {
              // If user table is in DISABLED state and index table is in ENABLED state means master
              // restarted as soon as user table disabled. So here we need to disable index table.
              new DisableTableHandler(master, Bytes.toBytes(indexTableName),
                  master.getCatalogTracker(), am, false).process();
              // clear index table region plans in secondary index load balancer.
              clearRegionPlans((HMaster) master, indexTableName);

            }
          }
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Balancing after master initialization.");
    }

    try {
      master.getAssignmentManager().waitUntilNoRegionsInTransition(timeout);
    } catch (InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Interrupted while waiting for the regions in transition to complete.", e);
      }
    }
    ((HMaster) master).balanceInternals();
    LOG.info("Exiting from preMasterInitialization.");
  }

  public static FileStatus getTableInfoPath(final FileSystem fs, final Path tabledir)
      throws IOException {
    FileStatus[] status = FSUtils.listStatus(fs, tabledir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        // Accept any file that starts with TABLEINFO_NAME
        return p.getName().startsWith(FSTableDescriptors.TABLEINFO_NAME);
      }
    });
    if (status == null || status.length < 1) return null;
    Arrays.sort(status, new FileStatusFileNameComparator());
    if (status.length > 1) {
      // Clean away old versions of .tableinfo
      for (int i = 1; i < status.length; i++) {
        Path p = status[i].getPath();
        // Clean up old versions
        if (!fs.delete(p, false)) {
          LOG.warn("Failed cleanup of " + p);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Cleaned up old tableinfo file " + p);
          }
        }
      }
    }
    return status[0];
  }

  /**
   * Compare {@link FileStatus} instances by {@link Path#getName()}. Returns in reverse order.
   */
  private static class FileStatusFileNameComparator implements Comparator<FileStatus> {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return -left.compareTo(right);
    }
  }
}
