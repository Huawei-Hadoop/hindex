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
package org.apache.hadoop.hbase.index.coprocessor.master;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.SecIndexLoadBalancer;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionSplitPolicy;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKTable;

/**
 * 
 * Defines of coprocessor hooks(to support secondary indexing) of operations on
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 * 
 */

public class IndexMasterObserver extends BaseMasterObserver {

  private static final Log LOG = LogFactory.getLog(IndexMasterObserver.class.getName());

  IndexManager idxManager = IndexManager.getInstance();

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
    byte[] indexBytes = desc.getValue(Constants.INDEX_SPEC_KEY);
    if (indexBytes != null) {
      Map<Column, Pair<ValueType, Integer>> indexColDetails =
          new HashMap<Column, Pair<ValueType, Integer>>();
      TableName tableName = desc.getTableName();
      checkEndsWithIndexSuffix(tableName);
      TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
      TableIndices tableIndices = new TableIndices();
      tableIndices.readFields(indexBytes);
      List<IndexSpecification> indices = tableIndices.getIndices();
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

      boolean isTableExists =
          MetaReader.tableExists(master.getCatalogTracker(), desc.getTableName());
      boolean isIndexTableExists =
          MetaReader.tableExists(master.getCatalogTracker(), indexTableName);

      if (isTableExists && isIndexTableExists) {
        throw new TableExistsException(desc.getTableName());
      } else if (isIndexTableExists) {
        disableAndDeleteTable(master, indexTableName);
      }
      idxManager.addIndexForTable(desc.getNameAsString(), indices);
    }
    LOG.info("Exiting from preCreateTable.");
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName, HTableDescriptor htd) throws IOException {
    String table = tableName.getNameAsString();
    MasterServices master = ctx.getEnvironment().getMasterServices();
    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations = null;
    LOG.info("Entering postModifyTable for the table " + table);
    byte[] indexBytes = htd.getValue(Constants.INDEX_SPEC_KEY);
    if (indexBytes != null) {
      TableDescriptors tableDescriptors = master.getTableDescriptors();
      Map<String, HTableDescriptor> allTableDesc = tableDescriptors.getAll();
      String indexTableName = IndexUtils.getIndexTableName(table);
      if (allTableDesc.containsKey(indexTableName)) {
        // Do table modification
        TableIndices tableIndices = new TableIndices();
        tableIndices.readFields(indexBytes);
        List<IndexSpecification> indices = tableIndices.getIndices();
        if (indices.isEmpty()) {
          LOG.error("Empty indices are passed to modify the table " + table);
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

          byte[][] splitKeys = getSplitKeys(regionInfo);
          createSecondaryIndexTable(htd, splitKeys, master, true);
        }
      }
    }
    LOG.info("Exiting postModifyTable for the table " + table);
  }

  private void checkColumnsForValidityAndConsistency(HTableDescriptor desc,
      IndexSpecification iSpec, Map<Column, Pair<ValueType, Integer>> indexColDetails)
      throws IOException {
    Set<ColumnQualifier> cqList = iSpec.getIndexColumns();
    if (cqList.isEmpty()) {
      String message =
          " Index " + iSpec.getName()
              + " doesn't contain any columns. Each index should contain atleast one column.";
      LOG.error(message);
      throw new DoNotRetryIOException(new IllegalArgumentException(message));
    }
    for (ColumnQualifier cq : cqList) {
      if (null == desc.getFamily(cq.getColumnFamily())) {
        String message =
            "Column family " + cq.getColumnFamilyString() + " in index specification "
                + iSpec.getName() + " not in Column families of table " + desc.getNameAsString()
                + '.';
        LOG.error(message);
        throw new DoNotRetryIOException(new IllegalArgumentException(message));
      }
      Column column = new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition());
      ValueType type = cq.getType();
      int maxlength = cq.getMaxValueLength();
      Pair<ValueType, Integer> colDetail = indexColDetails.get(column);
      if (null != colDetail) {
        if (!colDetail.getFirst().equals(type) || colDetail.getSecond() != maxlength) {
          throw new DoNotRetryIOException(new IllegalArgumentException(
              "ValueType/max value length of column " + column
                  + " not consistent across the indices"));
        }
      } else {
        indexColDetails.put(column, new Pair<ValueType, Integer>(type, maxlength));
      }
    }
  }

  private void checkEndsWithIndexSuffix(TableName tableName) throws IOException {
    if (IndexUtils.isIndexTable(tableName)) {
      String message =
          "User table name should not be ends with " + Constants.INDEX_TABLE_SUFFIX + '.';
      LOG.error(message);
      throw new DoNotRetryIOException(new IllegalArgumentException(message));
    }
  }

  private void disableAndDeleteTable(MasterServices master, TableName tableName) throws IOException {
    LOG.error(tableName + " already exists.  Disabling and deleting table " + tableName + '.');
    boolean disabled = master.getAssignmentManager().getZKTable().isDisabledTable(tableName);
    if (false == disabled) {
      LOG.info("Disabling table " + tableName + '.');
      new DisableTableHandler(master, tableName, master.getCatalogTracker(),
          master.getAssignmentManager(), master.getTableLockManager(), false).prepare().process();
      if (false == master.getAssignmentManager().getZKTable().isDisabledTable(tableName)) {
        throw new DoNotRetryIOException("Table " + tableName + " not disabled.");
      }
    }
    LOG.info("Disabled table " + tableName + '.');
    LOG.info("Deleting table " + tableName + '.');
    new DeleteTableHandler(tableName, master, master).prepare().process();
    if (true == MetaReader.tableExists(master.getCatalogTracker(), tableName)) {
      throw new DoNotRetryIOException("Table " + tableName + " not  deleted.");
    }
    LOG.info("Deleted table " + tableName + '.');
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    if (desc.getValue(Constants.INDEX_SPEC_KEY) != null) {
      LoadBalancer balancer =
          ctx.getEnvironment().getMasterServices().getAssignmentManager().getBalancer();
      if (balancer instanceof SecIndexLoadBalancer) {
        ((SecIndexLoadBalancer) balancer).addIndexedTable(desc.getTableName());
      }
    }
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    LOG.info("Entered into postCreateTableHandler of table " + desc.getNameAsString() + '.');
    if (idxManager.getIndicesForTable(desc.getNameAsString()) != null) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      byte[][] splitKeys = getSplitKeys(regions);
      // In case of post call for the index table creation, it wont be
      // IndexedHTableDescriptor
      createSecondaryIndexTable(desc, splitKeys, master, false);
      // if there is any user scenarios
      // we can add index datails to index manager
    }
    LOG.info("Exiting from postCreateTableHandler of table " + desc.getNameAsString() + '.');
  }

  /**
   * @param HTableDescriptor desc
   * @param HRegionInfo [] regions
   * @param MasterServices master
   * @throws NotAllMetaRegionsOnlineException
   * @throws IOException
   */
  private void createSecondaryIndexTable(HTableDescriptor desc, byte[][] splitKeys,
      MasterServices master, boolean disableTable) throws NotAllMetaRegionsOnlineException,
      IOException {
    TableName indexTableName =
        TableName.valueOf(IndexUtils.getIndexTableName(desc.getNameAsString()));
    LOG.info("Creating secondary index table " + indexTableName + " for table "
        + desc.getNameAsString() + '.');
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

    // TODO read this data from a config file??
    columnDescriptor.setBlocksize(8 * 1024);// 8KB

    indexTableDesc.addFamily(columnDescriptor);
    indexTableDesc.setValue(HTableDescriptor.SPLIT_POLICY, IndexRegionSplitPolicy.class.getName());
    LOG.info("Setting the split policy for the Index Table " + indexTableName + " as "
        + IndexRegionSplitPolicy.class.getName() + '.');
    HRegionInfo[] newRegions = getHRegionInfos(indexTableDesc, splitKeys);
    CreateTableHandler tableHandler =
        new CreateTableHandler(master, master.getMasterFileSystem(), indexTableDesc,
            master.getConfiguration(), newRegions, master);
    tableHandler.prepare();
    tableHandler.process();
    // Disable the index table so that when we enable the main table both can be enabled
    if (disableTable) {
      new DisableTableHandler(master, indexTableName, master.getCatalogTracker(),
          master.getAssignmentManager(), master.getTableLockManager(), false).prepare().process();
    }
    LOG.info("Created secondary index table " + indexTableName + " for table "
        + desc.getNameAsString() + '.');
  }

  private byte[][] getSplitKeys(HRegionInfo[] regions) {
    byte[][] splitKeys = null;
    if (null != regions && regions.length > 1) {
      // for the 1st region always the start key will be empty. We no need to
      // pass this as a start key item for the index table because this will
      // be added by HBase any way. So if we pass empty, HBase will create one
      // extra region with start and end key as empty byte[].
      splitKeys = new byte[regions.length - 1][];
      int i = 0;
      for (HRegionInfo region : regions) {
        byte[] startKey = region.getStartKey();
        if (startKey.length > 0) {
          splitKeys[i++] = startKey;
        }
      }
    }
    return splitKeys;
  }

  private HRegionInfo[] getHRegionInfos(HTableDescriptor hTableDescriptor, byte[][] splitKeys) {
    HRegionInfo[] hRegionInfos = null;
    if (splitKeys == null || splitKeys.length == 0) {
      hRegionInfos = new HRegionInfo[] { new HRegionInfo(hTableDescriptor.getTableName()) };
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] = new HRegionInfo(hTableDescriptor.getTableName(), startKey, endKey);
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

    if (!IndexUtils.isIndexTable(regionInfo.getTable().getName())) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      LoadBalancer balancer = master.getAssignmentManager().getBalancer();
      AssignmentManager am = master.getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      // waiting until user region is removed from transition.
      long timeout =
          master.getConfiguration()
              .getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
      try {
        am.waitOnRegionToClearRegionsInTransition(regionInfo, timeout);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while region in assignment.");
        }
      }
      ServerName sn = regionStates.getRegionServerOfRegion(regionInfo);
      TableName indexTableName =
          TableName.valueOf(IndexUtils.getIndexTableName(regionInfo.getTableName()));
      List<HRegionInfo> tableRegions = regionStates.getRegionsOfTable(indexTableName);
      for (HRegionInfo hRegionInfo : tableRegions) {
        if (0 == Bytes.compareTo(hRegionInfo.getStartKey(), regionInfo.getStartKey())) {
          am.addPlan(hRegionInfo.getEncodedName(), new RegionPlan(hRegionInfo, null, sn));
          LOG.info("Assigning region " + hRegionInfo.getRegionNameAsString() + " to server " + sn
              + '.');
          balancer.regionOnline(hRegionInfo, sn);
          am.assign(hRegionInfo, true, false);
          break;
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
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    String tableName = hri.getTable().getNameAsString();
    if (!IndexUtils.isIndexTable(tableName)) {
      if (regionStates.isRegionInTransition(hri)) {
        return true;
      } else {
        String indexTableName = IndexUtils.getIndexTableName(tableName);
        for (Entry<String, RegionState> region : regionStates.getRegionsInTransition().entrySet()) {
          HRegionInfo regionInfo = region.getValue().getRegion();
          if (indexTableName.equals(regionInfo.getTable().getNameAsString())) {
            if (Bytes.compareTo(hri.getStartKey(), regionInfo.getStartKey()) == 0) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo,
      ServerName srcServer, ServerName destServer) throws IOException {
    LOG.info("Entering into postMove " + regionInfo.getRegionNameAsString() + '.');
    if (!IndexUtils.isIndexTable(regionInfo.getTable().getNameAsString())) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      AssignmentManager am = master.getAssignmentManager();
      // waiting until user region is removed from transition.
      long timeout =
          master.getConfiguration()
              .getLong("hbase.bulk.assignment.waiton.empty.rit", 5 * 60 * 1000);
      try {
        am.waitOnRegionToClearRegionsInTransition(regionInfo, timeout);
        destServer = am.getRegionStates().getRegionServerOfRegion(regionInfo);
        am.getBalancer().regionOnline(regionInfo, destServer);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Interrupted while region in assignment.");
        }
      }
      TableName indexTableName =
          TableName.valueOf(IndexUtils.getIndexTableName(regionInfo.getTable().getNameAsString()));
      List<HRegionInfo> tableRegions = am.getRegionStates().getRegionsOfTable(indexTableName);
      for (HRegionInfo indexRegionInfo : tableRegions) {
        if (0 == Bytes.compareTo(indexRegionInfo.getStartKey(), regionInfo.getStartKey())) {
          LOG.info("Assigning region " + indexRegionInfo.getRegionNameAsString() + "from "
              + srcServer + " to server " + destServer + '.');
          am.getBalancer().regionOnline(indexRegionInfo, destServer);
          am.addPlan(indexRegionInfo.getEncodedName(), new RegionPlan(indexRegionInfo, null,
              destServer));
          am.unassign(indexRegionInfo);
        }
      }
    }
    LOG.info("Exiting from postMove " + regionInfo.getRegionNameAsString() + '.');
  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    LOG.info("Entered into postDisableTableHandler of table " + tableName);
    MasterServices master = ctx.getEnvironment().getMasterServices();
    AssignmentManager am = master.getAssignmentManager();
    try {
      if (!IndexUtils.isIndexTable(tableName.getNameAsString())) {
        TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
        // Index table may not present following three cases.
        // 1) Index details are not specified during table creation then index table wont be
        // created.
        // 2) Even we specify index details if master restarted in the middle of user table creation
        // corresponding index table wont be created. But without creating index table user table
        // wont
        // be disabled. No need to call disable for index table at that time.
        // 3) Index table may be deleted but this wont happen without deleting user table.
        if (am.getZKTable().isTablePresent(indexTableName)) {
          long timeout =
              master.getConfiguration().getLong("hbase.bulk.assignment.waiton.empty.rit",
                5 * 60 * 1000);
          // Both user table and index table should not be in enabling/disabling state at a time.
          // If disable is progress for user table then index table should be in ENABLED state.
          // If enable is progress for index table wait until table enabled.
          if (waitUntilTableEnabled(timeout, indexTableName, am.getZKTable())) {
            new DisableTableHandler(master, indexTableName, master.getCatalogTracker(), am,
                master.getTableLockManager(), false).process();
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table " + indexTableName + " not in ENABLED state to disable.");
            }
          }
        }
      }
    } finally {
      // clear user table region plans in secondary index load balancer.
      clearRegionPlans((HMaster) master, tableName.getNamespaceAsString());
    }
    LOG.info("Exiting from postDisableTableHandler of table " + tableName);
  }

  private void clearRegionPlans(HMaster master, String tableName) {
    AssignmentManager am = master.getAssignmentManager();
    if (am.getBalancer() instanceof SecIndexLoadBalancer) {
      ((SecIndexLoadBalancer) am.getBalancer()).clearTableRegionPlans(tableName);
    }
  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    LOG.info("Entered into postEnableTableHandler of table " + tableName);
    if (!IndexUtils.isIndexTable(tableName.getNameAsString())) {
      MasterServices master = ctx.getEnvironment().getMasterServices();
      AssignmentManager am = master.getAssignmentManager();
      TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
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
          new EnableTableHandler(master, indexTableName, master.getCatalogTracker(), am,
              master.getTableLockManager(), false).prepare().process();
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Table " + indexTableName + " not in DISABLED state to enable.");
          }
        }
      }
    }
    LOG.info("Exiting from postEnableTableHandler of table " + tableName);

  }

  private boolean waitUntilTableDisabled(long timeout, TableName tableName, ZKTable zk) {
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

  private boolean waitUntilTableEnabled(long timeout, TableName tableName, ZKTable zk) {
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
      TableName tableName) throws IOException {
    LOG.info("Entered into postDeleteTableHandler of table " + tableName + '.');
    MasterServices master = ctx.getEnvironment().getMasterServices();
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    boolean indexTablePresent =
        master.getAssignmentManager().getZKTable().isTablePresent(indexTableName);
    // Not checking for disabled state because before deleting user table both user and index table
    // should be disabled.
    if ((!IndexUtils.isIndexTable(tableName)) && indexTablePresent) {
      LoadBalancer balancer = master.getAssignmentManager().getBalancer();
      if (balancer instanceof SecIndexLoadBalancer) {
        ((SecIndexLoadBalancer) balancer).removeIndexedTable(tableName);
      }
      DeleteTableHandler dth = new DeleteTableHandler(indexTableName, master, master);
      dth.prepare();
      dth.process();
    }
    LOG.info("Exiting from postDeleteTableHandler of table " + tableName + '.');
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
    for (HTableDescriptor htd : htds) {
      if (!IndexUtils.isIndexTable(htd.getNameAsString()) && !htd.isMetaRegion()
          && !htd.getTableName().isSystemTable()) {
        if (htd.getValue(Constants.INDEX_SPEC_KEY) != null) {
          TableName tableName = htd.getTableName();
          LoadBalancer balancer = master.getAssignmentManager().getBalancer();
          if (balancer instanceof SecIndexLoadBalancer) {
            ((SecIndexLoadBalancer) balancer).addIndexedTable(tableName);
          }
          TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
          // TODO: check table exists from table descriptors list.
          boolean tableExists = MetaReader.tableExists(master.getCatalogTracker(), tableName);
          boolean indexTableExists =
              MetaReader.tableExists(master.getCatalogTracker(), indexTableName);
          if (tableExists && !indexTableExists) {
            LOG.info("Table has index specification details but " + "no corresponding index table.");
            List<HRegionInfo> regions =
                MetaReader.getTableRegions(master.getCatalogTracker(), tableName);
            HRegionInfo[] regionsArray = new HRegionInfo[regions.size()];
            byte[][] splitKeys = getSplitKeys(regions.toArray(regionsArray));
            createSecondaryIndexTable(htd, splitKeys, master, zkTable.isDisabledTable(tableName));
          } else if (true == tableExists && true == indexTableExists) {
            // If both tables are present both should be in same state in zookeeper. If tables are
            // partially enabled or disabled they will be processed as part of recovery
            // enabling/disabling tables.
            // if user table is in ENABLED state and index table is in DISABLED state means master
            // restarted as soon as user table enabled. So here we need to enable index table.
            if (zkTable.isEnabledTable(tableName) && zkTable.isDisabledTable(indexTableName)) {
              new EnableTableHandler(master, indexTableName, master.getCatalogTracker(), am,
                  master.getTableLockManager(), false).prepare().process();
            } else if (zkTable.isDisabledTable(tableName) && zkTable.isEnabledTable(indexTableName)) {
              // If user table is in DISABLED state and index table is in ENABLED state means master
              // restarted as soon as user table disabled. So here we need to disable index table.
              new DisableTableHandler(master, indexTableName, master.getCatalogTracker(), am,
                  master.getTableLockManager(), false).prepare().process();
              // clear index table region plans in secondary index load balancer.
              clearRegionPlans((HMaster) master, indexTableName.getNameAsString());
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

}
