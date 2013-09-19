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
package org.apache.hadoop.hbase.index.coprocessor.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserverExt;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.SplitTransaction.SplitInfo;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class IndexRegionObserver extends BaseRegionObserver implements RegionObserverExt {

  private static final Log LOG = LogFactory.getLog(IndexRegionObserver.class);

  // variable will be set to true in test case for testing
  // All below public static fields are used for testing.
  static boolean isTestingEnabled = false;

  public static boolean isSeekpointAddded = false;

  public static boolean isIndexedFlowUsed = false;

  public static List<byte[]> seekPoints = null;

  public static List<byte[]> seekPointsForMultipleIndices = null;

  private Map<RegionScanner, SeekPointFetcher> scannerMap =
      new ConcurrentHashMap<RegionScanner, SeekPointFetcher>();

  private IndexManager indexManager = IndexManager.getInstance();

  public static final ThreadLocal<IndexEdits> threadLocal = new ThreadLocal<IndexEdits>() {
    @Override
    protected IndexEdits initialValue() {
      return new IndexEdits();
    }
  };

  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> contx) {
    byte[] tableName = contx.getEnvironment().getRegion().getTableDesc().getName();
    String tableNameStr = Bytes.toString(tableName);
    if (IndexUtils.isCatalogTable(tableName) || IndexUtils.isIndexTable(tableNameStr)) {
      return;
    }
    LOG.trace("Entering postOpen for the table " + tableNameStr);
    this.indexManager.incrementRegionCount(tableNameStr);
    List<IndexSpecification> list = indexManager.getIndicesForTable(tableNameStr);
    if (null != list) {
      LOG.trace("Index Manager already contains an entry for the table "
          + ". Hence returning from postOpen");
      return;
    }
    RegionServerServices rss = contx.getEnvironment().getRegionServerServices();
    Configuration conf = rss.getConfiguration();
    IndexedHTableDescriptor tableDescriptor = null;
    try {
      tableDescriptor = IndexUtils.getIndexedHTableDescriptor(tableName, conf);
    } catch (IOException e) {
      rss.abort("Some unidentified scenario while reading from the "
          + "table descriptor . Aborting RegionServer", e);
    }
    if (tableDescriptor != null) {
      list = tableDescriptor.getIndices();
      if (list != null && list.size() > 0) {
        indexManager.addIndexForTable(tableNameStr, list);
        LOG.trace("Added index Specification in the Manager for the " + tableNameStr);
      } else {
        list = new ArrayList<IndexSpecification>();
        indexManager.addIndexForTable(tableNameStr, list);
        LOG.trace("Added index Specification in the Manager for the " + tableNameStr);
      }
    }
    LOG.trace("Exiting postOpen for the table " + tableNameStr);
  }

  @Override
  public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final List<Pair<Mutation, OperationStatus>> mutationVsBatchOp, final WALEdit edit)
      throws IOException {
    HRegionServer rs = (HRegionServer) ctx.getEnvironment().getRegionServerServices();
    HRegion userRegion = ctx.getEnvironment().getRegion();
    HTableDescriptor userTableDesc = userRegion.getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (IndexUtils.isCatalogTable(userTableDesc.getName()) || IndexUtils.isIndexTable(tableName)) {
      return;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName);
    if (indices == null || indices.isEmpty()) {
      LOG.trace("skipping preBatchMutate for the table " + tableName + " as there are no indices");
      return;
    }
    LOG.trace("Entering preBatchMutate for the table " + tableName);
    LOG.trace("Indices for the table " + tableName + " are: " + indices);
    HRegion indexRegion = getIndexTableRegion(tableName, userRegion, rs);
    // Storing this found HRegion in the index table within the thread locale.
    IndexEdits indexEdits = threadLocal.get();
    indexEdits.indexRegion = indexRegion;
    for (Pair<Mutation, OperationStatus> mutation : mutationVsBatchOp) {
      if (mutation.getSecond().getOperationStatusCode() != OperationStatusCode.NOT_RUN) {
        continue;
      }
      // only for successful puts
      Mutation m = mutation.getFirst();
      if (m instanceof Put) {
        try {
          prepareIndexMutations(indices, userRegion, m, tableName, indexRegion);
        } catch (IOException e) {
          mutation.setSecond(new OperationStatus(OperationStatusCode.SANITY_CHECK_FAILURE, e
              .getMessage()));
        }
      } else if (m instanceof Delete) {
        prepareIndexMutations(indices, userRegion, m, tableName, indexRegion);
      }
    }
    indexEdits.setUpdateLocked();
    indexRegion.updateLock();
    LOG.trace("Exiting preBatchMutate for the table " + tableName);
  }

  private HRegion getIndexTableRegion(String tableName, HRegion userRegion, HRegionServer rs)
      throws IOException {
    String indexTableName = IndexUtils.getIndexTableName(tableName);
    Collection<HRegion> idxTabRegions = rs.getOnlineRegions(Bytes.toBytes(indexTableName));
    for (HRegion idxTabRegion : idxTabRegions) {
      // TODO start key check is enough? May be we can check for the
      // possibility for N-1 Mapping?
      if (Bytes.equals(idxTabRegion.getStartKey(), userRegion.getStartKey())) {
        return idxTabRegion;
      }
    }
    // No corresponding index region found in the RS online regions list!
    LOG.warn("Index Region not found on the region server . "
        + "So skipping the put. Need Balancing");
    // TODO give a proper Exception msg
    throw new IOException();
  }

  private void prepareIndexMutations(List<IndexSpecification> indices, HRegion userRegion,
      Mutation mutation, String tableName, HRegion indexRegion) throws IOException {
    IndexEdits indexEdits = threadLocal.get();
    if (mutation instanceof Put) {
      for (IndexSpecification index : indices) {
        // Handle each of the index
        Mutation indexPut = IndexUtils.prepareIndexPut((Put) mutation, index, indexRegion);
        if (null != indexPut) {
          // This mutation can be null when the user table mutation is not
          // containing all of the indexed col value.
          indexEdits.add(indexPut);
        }
      }
    } else if (mutation instanceof Delete) {
      Collection<? extends Mutation> indexDeletes =
          prepareIndexDeletes((Delete) mutation, userRegion, indices, indexRegion);
      indexEdits.addAll(indexDeletes);
    } else {
      // TODO : Log or throw exception
    }
  }

  Collection<? extends Mutation> prepareIndexDeletes(Delete delete, HRegion userRegion,
      List<IndexSpecification> indexSpecs, HRegion indexRegion) throws IOException {
    Collection<Delete> indexDeletes = new LinkedHashSet<Delete>();
    for (Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
      for (KeyValue kv : entry.getValue()) {
        indexDeletes.addAll(getIndexDeletes(indexSpecs, userRegion, indexRegion, kv));
      }
    }
    return indexDeletes;
  }

  private static Collection<Delete> getIndexDeletes(List<IndexSpecification> indexSpecs,
      HRegion userRegion, HRegion indexRegion, KeyValue deleteKV) throws IOException {
    Collection<Delete> indexDeletes = new LinkedHashSet<Delete>();
    List<IndexSpecification> indicesToUpdate = new LinkedList<IndexSpecification>();
    Multimap<Long, KeyValue> groupedKV =
        doGetAndGroupByTS(indexSpecs, userRegion, deleteKV, indicesToUpdate);

    // There can be multiple index kvs for each user kv
    // So, prepare all resultant index delete kvs for this user delete kv
    for (Entry<Long, Collection<KeyValue>> entry : groupedKV.asMap().entrySet()) {
      for (IndexSpecification index : indicesToUpdate) {
        ByteArrayBuilder indexRow =
            IndexUtils.getIndexRowKeyHeader(index, indexRegion.getStartKey(), deleteKV.getRow());
        boolean update = false;
        for (ColumnQualifier cq : index.getIndexColumns()) {
          KeyValue kvFound = null;
          for (KeyValue kv : entry.getValue()) {
            if (Bytes.equals(cq.getColumnFamily(), kv.getFamily())
                && Bytes.equals(cq.getQualifier(), kv.getQualifier())) {
              kvFound = kv;
              update = true;
              break;
            }
          }
          if (kvFound == null) {
            indexRow.position(indexRow.position() + cq.getMaxValueLength());
          } else {
            IndexUtils.updateRowKeyForKV(cq, kvFound, indexRow);
          }
        }
        if (update) {
          // Append the actual row key at the end of the index row key.
          indexRow.put(deleteKV.getRow());
          Delete idxDelete = new Delete(indexRow.array());
          if (deleteKV.isDeleteType()) {
            idxDelete
                .deleteColumn(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, entry.getKey());
          } else {
            idxDelete.deleteFamily(Constants.IDX_COL_FAMILY, entry.getKey());
          }
          idxDelete.setWriteToWAL(false);
          indexDeletes.add(idxDelete);
        }
      }
    }
    return indexDeletes;
  }

  private static Multimap<Long, KeyValue> doGetAndGroupByTS(List<IndexSpecification> indexSpecs,
      HRegion userRegion, KeyValue deleteKV, List<IndexSpecification> indicesToConsider)
      throws IOException {

    Get get = new Get(deleteKV.getRow());
    long maxTS = HConstants.LATEST_TIMESTAMP;

    if (deleteKV.getTimestamp() < maxTS) {
      // Add +1 to make the current get includes the timestamp
      maxTS = deleteKV.getTimestamp() + 1;
    }
    get.setTimeRange(HConstants.OLDEST_TIMESTAMP, maxTS);

    for (IndexSpecification index : indexSpecs) {
      // Get all indices involves this family/qualifier
      if (index.contains(deleteKV.getFamily(), deleteKV.getQualifier())) {
        indicesToConsider.add(index);
        for (ColumnQualifier cq : index.getIndexColumns()) {
          get.addColumn(cq.getColumnFamily(), cq.getQualifier());
        }
      }
    }
    if (deleteKV.isDeleteType()) {
      get.setMaxVersions(1);
    } else if (deleteKV.isDeleteColumnOrFamily()) {
      get.setMaxVersions();
    }
    List<KeyValue> userKVs = userRegion.get(get, 0).list();

    // Group KV based on timestamp
    Multimap<Long, KeyValue> groupedKV = HashMultimap.create();

    if (userKVs != null) {
      for (KeyValue userKV : userKVs) {
        groupedKV.put(userKV.getTimestamp(), userKV);
      }
    }
    return groupedKV;
  }

  // collection of edits for index table's memstore and WAL
  public static class IndexEdits {
    private WALEdit walEdit = new WALEdit();
    private HRegion indexRegion;
    private boolean updatesLocked = false;

    /**
     * Collection of mutations with locks. Locks will be null always as they not yet acquired for
     * index table.
     * @see HRegion#batchMutate(Pair[])
     */
    private List<Pair<Mutation, Integer>> mutations = new ArrayList<Pair<Mutation, Integer>>();

    public WALEdit getWALEdit() {
      return this.walEdit;
    }

    public boolean isUpdatesLocked() {
      return this.updatesLocked;
    }

    public void setUpdateLocked() {
      updatesLocked = true;
    }

    public void add(Mutation mutation) {
      // Check if WAL is disabled
      for (List<KeyValue> kvs : mutation.getFamilyMap().values()) {
        for (KeyValue kv : kvs) {
          this.walEdit.add(kv);
        }
      }
      // There is no lock acquired for index table. So, set it to null
      this.mutations.add(new Pair<Mutation, Integer>(mutation, null));
    }

    public void addAll(Collection<? extends Mutation> mutations) {
      for (Mutation mutation : mutations) {
        add(mutation);
      }
    }

    public List<Pair<Mutation, Integer>> getIndexMutations() {
      return this.mutations;
    }

    public HRegion getRegion() {
      return this.indexRegion;
    }
  }

  @Override
  public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final List<Mutation> mutations, WALEdit walEdit) {
    HTableDescriptor userTableDesc = ctx.getEnvironment().getRegion().getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (IndexUtils.isCatalogTable(userTableDesc.getName()) || IndexUtils.isIndexTable(tableName)) {
      return;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName);
    if (indices == null || indices.isEmpty()) {
      LOG.trace("skipping postBatchMutate for the table " + tableName + " as there are no indices");
      return;
    }
    LOG.trace("Entering postBatchMutate for the table " + tableName);
    IndexEdits indexEdits = threadLocal.get();
    List<Pair<Mutation, Integer>> indexMutations = indexEdits.getIndexMutations();

    if (indexMutations.size() == 0) {
      return;
    }
    HRegion hr = indexEdits.getRegion();
    LOG.trace("Updating index table " + hr.getRegionInfo().getTableNameAsString());
    try {
      hr.batchMutateForIndex(indexMutations.toArray(new Pair[indexMutations.size()]));
    } catch (IOException e) {
      // TODO This can come? If so we need to revert the actual put
      // and make the op failed.
      LOG.error("Error putting data into the index region", e);
    }
    LOG.trace("Exiting postBatchMutate for the table " + tableName);
  }

  @Override
  public void postCompleteBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Mutation> mutations) throws IOException {
    IndexEdits indexEdits = threadLocal.get();
    if (indexEdits != null) {
      if (indexEdits.isUpdatesLocked()) {
        indexEdits.getRegion().releaseLock();
      }

    }
    threadLocal.remove();

  }

  @Override
  public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> e,
      InternalScanner s, byte[] currentRow, boolean hasMore) throws IOException {
    String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
    if (IndexUtils.isIndexTable(tableName)) {
      return true;
    }
    SeekAndReadRegionScanner bsrs = SeekAndReadRegionScannerHolder.getRegionScanner();
    if (bsrs != null) {
      while (false == bsrs.seekToNextPoint()) {
        SeekPointFetcher seekPointFetcher = scannerMap.get(bsrs);
        if (null != seekPointFetcher) {
          List<byte[]> seekPoints = new ArrayList<byte[]>(1);
          seekPointFetcher.nextSeekPoints(seekPoints, 1);
          // TODO use return boolean?
          if (seekPoints.isEmpty()) {
            LOG.trace("No seekpoints are remaining hence returning..  ");
            return false;
          }
          bsrs.addSeekPoints(seekPoints);
          if (isTestingEnabled) {
            setSeekPoints(seekPoints);
            setSeekpointAdded(true);
            addSeekPoints(seekPoints);
          }
        } else {
          // This will happen for a region with no index
          break;
        }
      }
    }
    return true;
  }  

  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
      RegionScanner s) {
    HRegion region = e.getEnvironment().getRegion();
    String tableName = region.getTableDesc().getNameAsString();
    HRegionServer rs = (HRegionServer) e.getEnvironment().getRegionServerServices();
    // If the passed region is a region from an indexed table
    SeekAndReadRegionScanner bsrs = null;

    try {
      List<IndexSpecification> indexlist = IndexManager.getInstance().getIndicesForTable(tableName);
      if (indexlist != null) {
        if (indexlist == null || indexlist.isEmpty()) {
          // Not an indexed table. Just return.
          return s;
        }
        LOG.trace("Entering postScannerOpen for the table " + tableName);
        Collection<HRegion> onlineRegions = rs.getOnlineRegionsLocalContext();
        for (HRegion onlineIdxRegion : onlineRegions) {
          if (IndexUtils.isCatalogTable(Bytes.toBytes(onlineIdxRegion.getTableDesc()
              .getNameAsString()))) {
            continue;
          }
          if (onlineIdxRegion.equals(region)) {
            continue;
          }
          if (Bytes.equals(onlineIdxRegion.getStartKey(), region.getStartKey())
              && Bytes.equals(Bytes.toBytes(IndexUtils.getIndexTableName(region.getTableDesc()
                  .getNameAsString())), onlineIdxRegion.getTableDesc().getName())) {
            ScanFilterEvaluator mapper = new ScanFilterEvaluator();
            IndexRegionScanner indexScanner =
                mapper.evaluate(scan, indexlist, onlineIdxRegion.getStartKey(), onlineIdxRegion,
                  tableName);
            if (indexScanner == null) return s;
            SeekPointFetcher spf = new SeekPointFetcher(indexScanner);
            ReInitializableRegionScanner reinitializeScanner =
                new ReInitializableRegionScannerImpl(s, scan, spf);
            bsrs = new BackwardSeekableRegionScanner(reinitializeScanner, scan, region, null);
            scannerMap.put(bsrs, spf);
            LOG.trace("Scanner Map has " + scannerMap);
            break;
          }
        }
        LOG.trace("Exiting postScannerOpen for the table " + tableName);
      }
    } catch (Exception ex) {
      LOG.error("Exception occured in postScannerOpen for the table " + tableName, ex);
    }
    if (bsrs != null) {
      return bsrs;
    } else {
      return s;
    }
  }

  public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s,
      List<Result> results, int nbRows, boolean hasMore) throws IOException {
    HRegion region = e.getEnvironment().getRegion();
    String tableName = region.getTableDesc().getNameAsString();
    try {
      if (s instanceof SeekAndReadRegionScanner) {
        LOG.trace("Entering preScannerNext for the table " + tableName);
        BackwardSeekableRegionScanner bsrs = (BackwardSeekableRegionScanner) s;
        SeekAndReadRegionScannerHolder.setRegionScanner(bsrs);
        SeekPointFetcher spf = scannerMap.get(bsrs);
        List<byte[]> seekPoints = null;
        if (spf != null) {
          if (isTestingEnabled) {
            setIndexedFlowUsed(true);
          }
          seekPoints = new ArrayList<byte[]>();
          spf.nextSeekPoints(seekPoints, nbRows);
        }
        if (seekPoints == null || seekPoints.isEmpty()) {
          LOG.trace("No seekpoints are remaining hence returning..  ");
          SeekAndReadRegionScannerHolder.removeRegionScanner();
          e.bypass();
          return false;
        }
        bsrs.addSeekPoints(seekPoints);
        // This setting is just for testing purpose
        if (isTestingEnabled) {
          setSeekPoints(seekPoints);
          setSeekpointAdded(true);
          addSeekPoints(seekPoints);
        }
        LOG.trace("Exiting preScannerNext for the table " + tableName);
      }
    } catch (Exception ex) {
      LOG.error("Exception occured in preScannerNext for the table " + tableName + ex);
    }
    return true;
  }

  @Override
  public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e,
      InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
    if (s instanceof SeekAndReadRegionScanner) {
      SeekAndReadRegionScannerHolder.removeRegionScanner();
    }
    return true;
  }

  @Override
  public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s)
      throws IOException {
    if (s instanceof BackwardSeekableRegionScanner) {
      scannerMap.remove((RegionScanner) s);
    }
  }

  @Override
  public SplitInfo preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> e,
      byte[] splitKey) throws IOException {
    RegionCoprocessorEnvironment environment = e.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    String userTableName = region.getTableDesc().getNameAsString();
    LOG.trace("Entering preSplitBeforePONR for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    String indexTableName = IndexUtils.getIndexTableName(userTableName);
    if (indexManager.getIndicesForTable(userTableName) != null) {
      HRegion indexRegion = null;
      SplitTransaction st = null;
      try {
        indexRegion = getIndexRegion(rs, region.getStartKey(), indexTableName);
        if (null != indexRegion) {
          LOG.info("Flushing the cache for the index table " + indexTableName + " for the region "
              + indexRegion.getRegionInfo());
          indexRegion.flushcache();
          if (LOG.isInfoEnabled()) {
            LOG.info("Forcing split for the index table " + indexTableName + " with split key "
                + Bytes.toString(splitKey));
          }
          st = new SplitTransaction(indexRegion, splitKey);
          if (!st.prepare()) {
            LOG.error("Prepare for the index table " + indexTableName
                + " failed. So returning null. ");
            return null;
          }
          indexRegion.forceSplit(splitKey);
          PairOfSameType<HRegion> daughterRegions = st.stepsBeforeAddingPONR(rs, rs, false);
          SplitInfo splitInfo = splitThreadLocal.get();
          splitInfo.setDaughtersAndTransaction(daughterRegions, st);
          LOG.info("Daughter regions created for the index table " + indexTableName
              + " for the region " + indexRegion.getRegionInfo());
          return splitInfo;
        } else {
          LOG.error("IndexRegion for the table " + indexTableName + " is null. So returning null. ");
          return null;
        }
      } catch (Exception ex) {
        LOG.error("Error while spliting the indexTabRegion or not able to get the indexTabRegion:"
            + indexRegion != null ? indexRegion.getRegionName() : "", ex);
        st.rollback(rs, rs);
        return null;
      }
    }
    LOG.trace("Indexes for the table " + userTableName
        + " are null. So returning the empty SplitInfo");
    return new SplitInfo();
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    if (splitThreadLocal != null) {
      splitThreadLocal.remove();
      splitThreadLocal.set(new SplitInfo());
    }
  }

  private HRegion getIndexRegion(HRegionServer rs, byte[] startKey, String indexTableName)
      throws IOException {
    List<HRegion> indexTabRegions = rs.getOnlineRegions(Bytes.toBytes(indexTableName));
    for (HRegion indexRegion : indexTabRegions) {
      if (Bytes.equals(startKey, indexRegion.getStartKey())) {
        return indexRegion;
      }
    }
    return null;
  }

  @Override
  public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException {
    RegionCoprocessorEnvironment environment = ctx.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    String userTableName = region.getTableDesc().getNameAsString();
    String indexTableName = IndexUtils.getIndexTableName(userTableName);
    if (IndexUtils.isIndexTable(userTableName)) {
      return;
    }
    LOG.trace("Entering postSplit for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    IndexManager indexManager = IndexManager.getInstance();
    SplitTransaction splitTransaction = null;
    if (indexManager.getIndicesForTable(userTableName) != null) {
      try {
        SplitInfo splitInfo = splitThreadLocal.get();
        splitTransaction = splitInfo.getSplitTransaction();
        PairOfSameType<HRegion> daughters = splitInfo.getDaughters();
        if (splitTransaction != null && daughters != null) {
          splitTransaction.stepsAfterPONR(rs, rs, daughters);
          LOG.info("Daughter regions are opened and split transaction finished"
              + " for zknodes for index table " + indexTableName + " for the region "
              + region.getRegionInfo());
        }
      } catch (Exception ex) {
        String msg =
            "Splitting of index region has failed in stepsAfterPONR stage so aborting the server";
        LOG.error(msg, ex);
        rs.abort(msg);
      }
    }
  }

  // A thread local variable used to get the splitted region information of the index region.
  // This is needed becuase in order to do the PONR entry we need the info of the index
  // region's daughter entries.
  public static final ThreadLocal<SplitInfo> splitThreadLocal = new ThreadLocal<SplitInfo>() {
    protected SplitInfo initialValue() {
      return null;
    };
  };

  @Override
  public void preRollBack(ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
    RegionCoprocessorEnvironment environment = ctx.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    String userTableName = region.getTableDesc().getNameAsString();
    if (IndexUtils.isIndexTable(userTableName)) {
      return;
    }
    LOG.trace("Entering preRollBack for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    SplitInfo splitInfo = splitThreadLocal.get();
    SplitTransaction splitTransaction = splitInfo.getSplitTransaction();
    try {
      if (splitTransaction != null) {
        splitTransaction.rollback(rs, rs);
        LOG.info("preRollBack successfully done for the table " + userTableName
            + " for the region " + region.getRegionInfo());
      }
    } catch (Exception e) {
      LOG.error(
        "Error while rolling back the split failure for index region "
            + splitTransaction.getParent(), e);
      rs.abort("Abort; we got an error during rollback of index");
    }
  }

  // For testing to check whether final step of seek point is added
  public static void setSeekpointAdded(boolean isSeekpointAddded) {
    IndexRegionObserver.isSeekpointAddded = isSeekpointAddded;
  }

  // For testing
  public static boolean getSeekpointAdded() {
    return isSeekpointAddded;
  }

  // For testing to ensure indexed flow is used or not
  public static void setIndexedFlowUsed(boolean isIndexedFlowUsed) {
    IndexRegionObserver.isIndexedFlowUsed = isIndexedFlowUsed;
  }

  // For testing
  public static boolean getIndexedFlowUsed() {
    return isIndexedFlowUsed;
  }

  // For testing
  static List<byte[]> getSeekpoints() {
    return seekPoints;
  }

  // For testing to ensure cache size is returned correctly
  public static void setSeekPoints(List<byte[]> seekPoints) {
    IndexRegionObserver.seekPoints = seekPoints;
  }

  public static void setIsTestingEnabled(boolean isTestingEnabled) {
    IndexRegionObserver.isTestingEnabled = isTestingEnabled;
  }

  public static void addSeekPoints(List<byte[]> seekPoints) {
    if (seekPoints == null) {
      IndexRegionObserver.seekPointsForMultipleIndices = null;
      return;
    }
    if (IndexRegionObserver.seekPointsForMultipleIndices == null) {
      IndexRegionObserver.seekPointsForMultipleIndices = new ArrayList<byte[]>();
    }
    IndexRegionObserver.seekPointsForMultipleIndices.addAll(seekPoints);
  }

  public static List<byte[]> getMultipleSeekPoints() {
    return IndexRegionObserver.seekPointsForMultipleIndices;
  }

  private static class SeekAndReadRegionScannerHolder {
    private static ThreadLocal<SeekAndReadRegionScanner> holder =
        new ThreadLocal<SeekAndReadRegionScanner>();

    public static void setRegionScanner(SeekAndReadRegionScanner scanner) {
      holder.set(scanner);
    }

    public static SeekAndReadRegionScanner getRegionScanner() {
      return holder.get();
    }

    public static void removeRegionScanner() {
      holder.remove();
    }
  }

  @Override
  public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
      InternalScanner s) throws IOException {
    HRegionServer rs = (HRegionServer) c.getEnvironment().getRegionServerServices();
    if (!store.getTableName().contains(Constants.INDEX_TABLE_SUFFIX)) {
      // Not an index table
      return null;
    }
    long smallestReadPoint = c.getEnvironment().getRegion().getSmallestReadPoint();
    String actualTableName = IndexUtils.getActualTableNameFromIndexTableName(store.getTableName());
    TTLStoreScanner ttlStoreScanner =
        new TTLStoreScanner(store, smallestReadPoint, earliestPutTs, scanType, scanners,
            new TTLExpiryChecker(), actualTableName, rs);
    return ttlStoreScanner;
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
    HRegion region = e.getEnvironment().getRegion();
    byte[] tableName = region.getRegionInfo().getTableName();
    if (IndexUtils.isCatalogTable(tableName) || IndexUtils.isIndexTable(tableName)) {
      return;
    }
    if (splitThreadLocal.get() == null) {
      this.indexManager.decrementRegionCount(Bytes.toString(tableName), true);
    } else {
      this.indexManager.decrementRegionCount(Bytes.toString(tableName), false);
    }
  }

  private boolean isValidIndexMutation(HTableDescriptor userTableDesc, String tableName) {
    if (IndexUtils.isCatalogTable(userTableDesc.getName()) || IndexUtils.isIndexTable(tableName)) {
      return false;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName);
    if (indices == null || indices.isEmpty()) {
      LOG.trace("skipping preBatchMutate for the table " + tableName + " as there are no indices");
      return false;
    }
    return true;
  }

  private void acquireLockOnIndexRegion(String tableName, HRegion userRegion, HRegionServer rs)
      throws IOException {
    HRegion indexRegion = getIndexTableRegion(tableName, userRegion, rs);
    indexRegion.checkResources();
    indexRegion.startRegionOperation();
  }

  @Override
  public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> e)
      throws IOException {
    HRegionServer rs = (HRegionServer) e.getEnvironment().getRegionServerServices();
    HRegion userRegion = e.getEnvironment().getRegion();
    HTableDescriptor userTableDesc = userRegion.getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (!isValidIndexMutation(userTableDesc, tableName)) {
      // Ideally need not release any lock because in the preStartRegionOperationHook we would not
      // have
      // acquired
      // any lock on the index region
      return;
    }
    HRegion indexRegion = getIndexTableRegion(tableName, userRegion, rs);
    // This check for isClosed and isClosing is needed because we should not unlock
    // when the index region lock would have already been released before throwing NSRE

    // TODO : What is the scenario that i may get an IllegalMonitorStateException
    if (!indexRegion.isClosed() || !indexRegion.isClosing()) {
      indexRegion.closeRegionOperation();
    }
  }

  @Override
  public void postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> e)
      throws IOException {
    HRegionServer rs = (HRegionServer) e.getEnvironment().getRegionServerServices();
    HRegion userRegion = e.getEnvironment().getRegion();
    HTableDescriptor userTableDesc = userRegion.getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (!isValidIndexMutation(userTableDesc, tableName)) {
      return;
    }
    acquireLockOnIndexRegion(tableName, userRegion, rs);

  }

}
