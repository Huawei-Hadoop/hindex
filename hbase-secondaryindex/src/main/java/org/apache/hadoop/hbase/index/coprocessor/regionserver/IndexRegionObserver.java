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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.exception.StaleRegionBoundaryException;
import org.apache.hadoop.hbase.index.io.IndexHalfStoreFileReader;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.Operation;
import org.apache.hadoop.hbase.regionserver.IndexSplitTransaction;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.SplitTransaction;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class IndexRegionObserver extends BaseRegionObserver {

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
    HTableDescriptor tableDesc = contx.getEnvironment().getRegion().getTableDesc();
    RegionServerServices rss = contx.getEnvironment().getRegionServerServices();
    TableName tableName = tableDesc.getTableName();
    if (isNotIndexedTableDescriptor(tableDesc)) {
      return;
    }
    LOG.trace("Entering postOpen for the table " + tableName);
    this.indexManager.incrementRegionCount(tableName.getNameAsString());
    List<IndexSpecification> list = indexManager.getIndicesForTable(tableName.getNameAsString());
    if (null != list) {
      LOG.trace("Index Manager already contains an entry for the table "
          + ". Hence returning from postOpen");
      return;
    }
    byte[] indexBytes = tableDesc.getValue(Constants.INDEX_SPEC_KEY);
    TableIndices tableIndices = new TableIndices();
    try {
      tableIndices.readFields(indexBytes);
    } catch (IOException e) {
      rss.abort("Some unidentified scenario while reading from the "
          + "table descriptor . Aborting RegionServer", e);
    }
    list = tableIndices.getIndices();
    if (list != null && list.size() > 0) {
      indexManager.addIndexForTable(tableName.getNameAsString(), list);
      LOG.trace("Added index Specification in the Manager for the " + tableName);
    }
    LOG.trace("Exiting postOpen for the table " + tableName);
  }

  @Override
  public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    HRegionServer rs = (HRegionServer) ctx.getEnvironment().getRegionServerServices();
    HRegion userRegion = ctx.getEnvironment().getRegion();
    HTableDescriptor userTableDesc = userRegion.getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (isNotIndexedTableDescriptor(userTableDesc)) {
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
    for (int i = 0; i < miniBatchOp.size(); i++) {
      Mutation m = miniBatchOp.getOperation(i);
      if (m instanceof Put) {
        try {
          prepareIndexMutations(indices, userRegion, m, tableName, indexRegion);
        } catch (IOException e) {
          miniBatchOp.setOperationStatus(i, new OperationStatus(
              OperationStatusCode.SANITY_CHECK_FAILURE, e.getMessage()));
        }
      } else if (m instanceof Delete) {
        prepareIndexMutations(indices, userRegion, m, tableName, indexRegion);
      }
    }
    indexEdits.setUpdateLocked();
    indexRegion.updatesLock();
    LOG.trace("Exiting preBatchMutate for the table " + tableName);
  }

  // TODO make this api generic and use in scanner open hook to find the index region.
  private HRegion getIndexTableRegion(String tableName, HRegion userRegion, HRegionServer rs)
      throws IOException {
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(tableName));
    Collection<HRegion> idxTabRegions = rs.getOnlineRegions(indexTableName);
    for (HRegion idxTabRegion : idxTabRegions) {
      // TODO start key check is enough? May be we can check for the
      // possibility for N-1 Mapping?
      if (Bytes.equals(idxTabRegion.getStartKey(), userRegion.getStartKey())) {
        return idxTabRegion;
      }
    }
    // No corresponding index region found in the RS online regions list!
    String message =
        "Index Region not found on the region server . " + "So skipping the put. Need Balancing";
    LOG.warn(message);
    // TODO give a proper Exception msg
    throw new DoNotRetryIOException(message);
  }

  private void prepareIndexMutations(List<IndexSpecification> indices, HRegion userRegion,
      Mutation mutation, String tableName, HRegion indexRegion) throws IOException {
    IndexEdits indexEdits = threadLocal.get();
    if (mutation instanceof Put) {
      for (IndexSpecification index : indices) {
        if (IndexSpecification.ARBITRARY_COL_IDX_NAME.equals(index.getName())) {
          // TODO some code duplication we can avoid
          List<Mutation> indexPuts = IndexUtils.preparePutsForArbitraryIndex((Put) mutation, index,
              indexRegion.getStartKey());
          for (Mutation m : indexPuts) {
            indexEdits.add(m);
          }
        } else {
          // Handle each of the index
          Mutation indexPut = IndexUtils.prepareIndexPut((Put) mutation, index, indexRegion);
          if (null != indexPut) {
            // This mutation can be null when the user table mutation is not
            // containing all of the indexed col value.
            indexEdits.add(indexPut);
          }
        }
      }
    } else if (mutation instanceof Delete) {
      // TODO handle ARBITRARY_COL_IDX_NAME
      Collection<? extends Mutation> indexDeletes =
          prepareIndexDeletes((Delete) mutation, userRegion, indices, indexRegion);
      indexEdits.addAll(indexDeletes);
    }
  }

  Collection<? extends Mutation> prepareIndexDeletes(Delete delete, HRegion userRegion,
      List<IndexSpecification> indexSpecs, HRegion indexRegion) throws IOException {
    Collection<Delete> indexDeletes = new LinkedHashSet<Delete>();
    for (Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        indexDeletes.addAll(getIndexDeletes(indexSpecs, userRegion, indexRegion,
          KeyValueUtil.ensureKeyValue(cell)));
      }
    }
    return indexDeletes;
  }

  private static Collection<Delete> getIndexDeletes(List<IndexSpecification> indexSpecs,
      HRegion userRegion, HRegion indexRegion, Cell deleteKV) throws IOException {
    Collection<Delete> indexDeletes = new LinkedHashSet<Delete>();
    List<IndexSpecification> indicesToUpdate = new LinkedList<IndexSpecification>();
    Multimap<Long, Cell> groupedKV =
        doGetAndGroupByTS(indexSpecs, userRegion, deleteKV, indicesToUpdate);

    // There can be multiple index kvs for each user kv
    // So, prepare all resultant index delete kvs for this user delete kv
    for (Entry<Long, Collection<Cell>> entry : groupedKV.asMap().entrySet()) {
      for (IndexSpecification index : indicesToUpdate) {
        ByteArrayBuilder indexRow =
            IndexUtils.getIndexRowKeyHeader(index, indexRegion.getStartKey(), deleteKV.getRow());
        boolean update = false;
        for (ColumnQualifier cq : index.getIndexColumns()) {
          Cell kvFound = null;
          for (Cell kv : entry.getValue()) {
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
          if (((KeyValue) deleteKV).isDeleteType()) {
            idxDelete
                .deleteColumn(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, entry.getKey());
          } else {
            idxDelete.deleteFamily(Constants.IDX_COL_FAMILY, entry.getKey());
          }
          indexDeletes.add(idxDelete);
        }
      }
    }
    return indexDeletes;
  }

  private static Multimap<Long, Cell> doGetAndGroupByTS(List<IndexSpecification> indexSpecs,
      HRegion userRegion, Cell deleteKV, List<IndexSpecification> indicesToConsider)
      throws IOException {

    Get get = new Get(deleteKV.getRow());
    long maxTS = HConstants.LATEST_TIMESTAMP;

    if (deleteKV.getTimestamp() < maxTS) {
      // Add +1 to make the current get includes the timestamp
      maxTS = deleteKV.getTimestamp() + 1;
    }
    get.setTimeRange(0L, maxTS);

    for (IndexSpecification index : indexSpecs) {
      // Get all indices involves this family/qualifier
      if (index.contains(deleteKV.getFamily(), deleteKV.getQualifier())) {
        indicesToConsider.add(index);
        for (ColumnQualifier cq : index.getIndexColumns()) {
          get.addColumn(cq.getColumnFamily(), cq.getQualifier());
        }
      }
    }
    if (((KeyValue) deleteKV).isDeleteType()) {
      get.setMaxVersions(1);
    } else if (((KeyValue) deleteKV).isDeleteColumnOrFamily()) {
      get.setMaxVersions();
    }
    List<KeyValue> userKVs = userRegion.get(get).list();

    // Group KV based on timestamp
    Multimap<Long, Cell> groupedKV = HashMultimap.create();

    if (userKVs != null) {
      for (Cell userKV : userKVs) {
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
    private List<Mutation> mutations = new ArrayList<Mutation>();

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
      for (List<? extends Cell> kvs : mutation.getFamilyCellMap().values()) {
        for (Cell cell : kvs) {
          this.walEdit.add(KeyValueUtil.ensureKeyValue(cell));
        }
      }
      // There is no lock acquired for index table. So, set it to null
      this.mutations.add(mutation);
    }

    public void addAll(Collection<? extends Mutation> mutations) {
      for (Mutation mutation : mutations) {
        add(mutation);
      }
    }

    public List<Mutation> getIndexMutations() {
      return this.mutations;
    }

    public HRegion getRegion() {
      return this.indexRegion;
    }
  }

  @Override
  public void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    HTableDescriptor userTableDesc = ctx.getEnvironment().getRegion().getTableDesc();
    String tableName = userTableDesc.getNameAsString();
    if (isNotIndexedTableDescriptor(userTableDesc)) {
      return;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName);
    if (indices == null || indices.isEmpty()) {
      LOG.trace("skipping postBatchMutate for the table " + tableName + " as there are no indices");
      return;
    }
    LOG.trace("Entering postBatchMutate for the table " + tableName);
    IndexEdits indexEdits = threadLocal.get();
    List<Mutation> indexMutations = indexEdits.getIndexMutations();

    if (indexMutations.size() == 0) {
      return;
    }
    HRegion hr = indexEdits.getRegion();
    try {
      hr.batchMutateForIndex(indexMutations.toArray(new Mutation[indexMutations.size()]));
    } catch (IOException e) {
      // TODO This can come? If so we need to revert the actual put
      // and make the op failed.
      LOG.error("Error putting data into the index region", e);
    }
    LOG.trace("Exiting postBatchMutate for the table " + tableName);
  }

  private boolean isNotIndexedTableDescriptor(HTableDescriptor userTableDesc) {
    TableName tableName = userTableDesc.getTableName();
    return IndexUtils.isCatalogOrSystemTable(tableName)
        || IndexUtils.isIndexTable(tableName)
        || (!IndexUtils.isIndexTable(tableName) && userTableDesc.getValue(Constants.INDEX_SPEC_KEY) == null);
  }

  @Override
  public void postBatchMutateIndispensably(ObserverContext<RegionCoprocessorEnvironment> ctx,
      MiniBatchOperationInProgress<Mutation> miniBatchOp, boolean success) throws IOException {
    IndexEdits indexEdits = threadLocal.get();
    if (indexEdits != null) {
      if (indexEdits.isUpdatesLocked()) {
        indexEdits.getRegion().updatesUnlock();
      }
    }
    threadLocal.remove();
  }

  @Override
  public boolean postScannerFilterRow(ObserverContext<RegionCoprocessorEnvironment> e,
      InternalScanner s, byte[] currentRow, final int offset, final short length, boolean hasMore)
      throws IOException {
    HTableDescriptor tableDesc = e.getEnvironment().getRegion().getTableDesc();
    if (isNotIndexedTableDescriptor(tableDesc)) {
      return hasMore;
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
      final RegionScanner s) throws IOException {
    HRegion region = e.getEnvironment().getRegion();
    HTableDescriptor tableDesc = region.getTableDesc();
    String tableName = tableDesc.getNameAsString();
    HRegionServer rs = (HRegionServer) e.getEnvironment().getRegionServerServices();
    if (isNotIndexedTableDescriptor(tableDesc)) {
      return s;
    }
    byte[] indexBytes = scan.getAttribute(Constants.BUILD_INDICES);
    if (indexBytes != null) {
      checkScanBoundaries(e.getEnvironment().getRegion(), scan, s);
      return buildIndex(e, s, region, indexBytes);
    }
    indexBytes = scan.getAttribute(Constants.DROP_INDICES);
    if (indexBytes != null) {
      checkScanBoundaries(e.getEnvironment().getRegion(), scan, s);
      return dropIndex(e, s, region, indexBytes);
    }
    // If the passed region is a region from an indexed table
    SeekAndReadRegionScanner bsrs = null;

    try {
      List<IndexSpecification> indexlist = IndexManager.getInstance().getIndicesForTable(tableName);
      if (indexlist == null || indexlist.isEmpty()) {
        // Not an indexed table. Just return.
        return s;
      }
      if (indexlist != null) {
        LOG.info("Entering postScannerOpen for the table " + tableName);
        Collection<HRegion> onlineRegions = ((HRegionServer) rs).getOnlineRegionsLocalContext();
        for (HRegion onlineIdxRegion : onlineRegions) {
          if (IndexUtils.isCatalogOrSystemTable(onlineIdxRegion.getTableDesc().getTableName())) {
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

  private void checkScanBoundaries(HRegion region, Scan scan, RegionScanner rs)
      throws StaleRegionBoundaryException {
    if (Bytes.compareTo(region.getStartKey(), scan.getStartRow()) != 0
        || Bytes.compareTo(region.getEndKey(), scan.getStopRow()) != 0) {
      try{
        rs.close();
      } catch (Throwable e) {
        // skip
      }
      throw new StaleRegionBoundaryException(
        "The region key range and scan row key range are not same.");
    }
  }

  private RegionScanner buildIndex(ObserverContext<RegionCoprocessorEnvironment> e, final RegionScanner s,
      HRegion region, byte[] indexesBytes) throws IOException {
    // TODO: do this under lock...other wise may face inconsistent results during split....
    TableIndices indices = new TableIndices();
    indices.readFields(indexesBytes);
    HRegion indexRegion = getIndexRegion(e.getEnvironment());
    boolean hasMore = false;
    try {
      e.getEnvironment().getRegion().startRegionOperation();
      do {
        // TODO: Write index puts into batches...than writing individual. Batch size can be
        // configurable.
        List<Cell> results = new ArrayList<Cell>();
        hasMore = s.nextRaw(results);
        if (!results.isEmpty()) {
          Put p =
              new Put(results.get(0).getRowArray(), results.get(0).getRowOffset(), results.get(0)
                  .getRowLength());
          for (Cell c : results) {
            p.add(c);
          }
          for (IndexSpecification spec : indices.getIndices()) {
            Put indexPut = IndexUtils.prepareIndexPut(p, spec, region.getStartKey());
            if(indexPut != null) {
              indexRegion.put(indexPut);
            }
          }
        }
      } while (hasMore);
    } finally {
      e.getEnvironment().getRegion().closeRegionOperation();
    }
    RegionScanner scanner = new RegionScanner() {
      
      @Override
      public boolean next(List<Cell> result, int limit) throws IOException {
        return false;
      }
      
      @Override
      public boolean next(List<Cell> results) throws IOException {
        return false;
      }
      
      @Override
      public void close() throws IOException {
        s.close();
      }
      
      @Override
      public boolean reseek(byte[] row) throws IOException {
        return false;
      }
      
      @Override
      public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return false;
      }
      
      @Override
      public boolean nextRaw(List<Cell> result) throws IOException {
        return false;
      }
      
      @Override
      public boolean isFilterDone() throws IOException {
        return false;
      }
      
      @Override
      public HRegionInfo getRegionInfo() {
        return s.getRegionInfo();
      }
      
      @Override
      public long getMvccReadPoint() {
        return s.getMvccReadPoint();
      }
      
      @Override
      public long getMaxResultSize() {
        return s.getMaxResultSize();
      }
    };
    return scanner;
  }

  private RegionScanner dropIndex(ObserverContext<RegionCoprocessorEnvironment> e, final RegionScanner s,
      HRegion region, byte[] indexesBytes) throws IOException {
    // TODO:2) do the deletes in batch...so it will be faster..
    TableIndices indices = new TableIndices();
    indices.readFields(indexesBytes);
    HRegion indexRegion = getIndexRegion(e.getEnvironment());
    try {
      e.getEnvironment().getRegion().startRegionOperation();
      for (IndexSpecification spec : indices.getIndices()) {
        Scan scan = new Scan();
        byte[] indexName = Bytes.toBytes(spec.getName());
        byte[] startKey = indexRegion.getRegionInfo().getStartKey();
        int rowLength = indexRegion.getRegionInfo().getStartKey().length + indexName.length;
        ByteArrayBuilder row = ByteArrayBuilder.allocate(rowLength + 1);
        row.put(startKey);
        row.position(row.position() + 1);
        row.put(indexName);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.setStartRow(row.array());
        scan.setStopRow(IndexUtils.incrementValue(row.array(), true));
        boolean hasNext = false;
        RegionScanner scanner = null;
        try {
          do {
            List<Cell> results = new ArrayList<Cell>();
            scanner = indexRegion.getScanner(scan);
            hasNext = scanner.next(results);
            if (!results.isEmpty()) {
              Delete d =
                  new Delete(results.get(0).getRowArray(), results.get(0).getRowOffset(), results
                      .get(0).getRowLength());
              indexRegion.delete(d);
            }
          } while (hasNext);
        } finally {
          if (scanner != null) scanner.close();
        }
      }
    } finally {
      e.getEnvironment().getRegion().closeRegionOperation();
    }
    RegionScanner scanner = new RegionScanner() {
      
      @Override
      public boolean next(List<Cell> result, int limit) throws IOException {
        return false;
      }
      
      @Override
      public boolean next(List<Cell> results) throws IOException {
        return false;
      }
      
      @Override
      public void close() throws IOException {
        s.close();
      }
      
      @Override
      public boolean reseek(byte[] row) throws IOException {
        return false;
      }
      
      @Override
      public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return false;
      }
      
      @Override
      public boolean nextRaw(List<Cell> result) throws IOException {
        return false;
      }
      
      @Override
      public boolean isFilterDone() throws IOException {
        return false;
      }
      
      @Override
      public HRegionInfo getRegionInfo() {
        return s.getRegionInfo();
      }
      
      @Override
      public long getMvccReadPoint() {
        return s.getMvccReadPoint();
      }
      
      @Override
      public long getMaxResultSize() {
        return s.getMaxResultSize();
      }
    };
    return scanner;
  }
  
  public static HRegion getIndexRegion(RegionCoprocessorEnvironment environment) throws IOException {
    HRegion userRegion = environment.getRegion();
    TableName indexTableName = TableName.valueOf(IndexUtils.getIndexTableName(userRegion.getTableDesc().getTableName()));
    List<HRegion> onlineRegions = environment.getRegionServerServices().getOnlineRegions(indexTableName);
    for(HRegion indexRegion : onlineRegions) {
        if (Bytes.compareTo(userRegion.getStartKey(), indexRegion.getStartKey()) == 0) {
            return indexRegion;
        }
    }
    return null;
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
  public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> e, byte[] splitKey,
      List<Mutation> metaEntries) throws IOException {
    RegionCoprocessorEnvironment environment = e.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    String userTableName = region.getTableDesc().getNameAsString();
    LOG.trace("Entering preSplitBeforePONR for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    String indexTableName = IndexUtils.getIndexTableName(userTableName);
    TableIndices tableIndices = new TableIndices();
    if (indexManager.getIndicesForTable(userTableName) == null) {
      byte[] indexBytes = region.getTableDesc().getValue(Constants.INDEX_SPEC_KEY);
      if (indexBytes == null) {
        return;
      }
      tableIndices.readFields(indexBytes);
      indexManager.addIndexForTable(userTableName, tableIndices.getIndices());
    }
    if (indexManager.getIndicesForTable(userTableName) != null) {
      HRegion indexRegion = null;
      IndexSplitTransaction st = null;
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
          st = new IndexSplitTransaction(indexRegion, splitKey);
          if (!st.prepare()) {
            LOG.error("Prepare for the index table " + indexTableName
                + " failed. So returning null. ");
            e.bypass();
            return;
          }
          indexRegion.forceSplit(splitKey);
          PairOfSameType<HRegion> daughterRegions = st.stepsBeforePONR(rs, rs, false);
          splitThreadLocal.set(new SplitInfo(indexRegion, daughterRegions, st));
          HRegionInfo copyOfParent = new HRegionInfo(indexRegion.getRegionInfo());
          copyOfParent.setOffline(true);
          copyOfParent.setSplit(true);
          // Put for parent
          Put putParent = MetaEditor.makePutFromRegionInfo(copyOfParent);
          MetaEditor.addDaughtersToPut(putParent, daughterRegions.getFirst().getRegionInfo(),
            daughterRegions.getSecond().getRegionInfo());
          metaEntries.add(putParent);
          // Puts for daughters
          Put putA = MetaEditor.makePutFromRegionInfo(daughterRegions.getFirst().getRegionInfo());
          Put putB = MetaEditor.makePutFromRegionInfo(daughterRegions.getSecond().getRegionInfo());
          st.addLocation(putA, rs.getServerName(), 1);
          st.addLocation(putB, rs.getServerName(), 1);
          metaEntries.add(putA);
          metaEntries.add(putB);
          LOG.info("Daughter regions created for the index table " + indexTableName
              + " for the region " + indexRegion.getRegionInfo());
          return;
        } else {
          LOG.error("IndexRegion for the table " + indexTableName + " is null. So returning null. ");
          e.bypass();
          return;
        }
      } catch (Exception ex) {
        LOG.error("Error while spliting the indexTabRegion or not able to get the indexTabRegion:"
            + indexRegion != null ? indexRegion.getRegionName() : "", ex);
        if (st != null) st.rollback(rs, rs);
        e.bypass();
        return;
      }
    }
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    if (splitThreadLocal != null) {
      splitThreadLocal.remove();
    }
  }

  private HRegion getIndexRegion(HRegionServer rs, byte[] startKey, String indexTableName)
      throws IOException {
    List<HRegion> indexTabRegions = rs.getOnlineRegions(TableName.valueOf(indexTableName));
    for (HRegion indexRegion : indexTabRegions) {
      if (Bytes.equals(startKey, indexRegion.getStartKey())) {
        return indexRegion;
      }
    }
    return null;
  }

  @Override
  public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    RegionCoprocessorEnvironment environment = e.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    String userTableName = region.getTableDesc().getNameAsString();
    String indexTableName = IndexUtils.getIndexTableName(userTableName);
    if (IndexUtils.isIndexTable(userTableName)) {
      return;
    }
    LOG.trace("Entering postSplit for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    IndexSplitTransaction splitTransaction = null;
    if (region.getTableDesc().getValue(Constants.INDEX_SPEC_KEY) != null) {
      try {
        SplitInfo splitInfo = splitThreadLocal.get();
        if (splitInfo == null) return;
        splitTransaction = splitInfo.getSplitTransaction();
        PairOfSameType<HRegion> daughters = splitInfo.getDaughters();
        if (splitTransaction != null && daughters != null) {
          splitTransaction.stepsAfterPONR(rs, rs, daughters);
          LOG.info("Daughter regions are opened and split transaction finished for zknodes for index table "
              + indexTableName + " for the region " + region.getRegionInfo());
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
  public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException {
    RegionCoprocessorEnvironment environment = ctx.getEnvironment();
    HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
    HRegion region = environment.getRegion();
    HTableDescriptor tableDesc = region.getTableDesc();
    String userTableName = tableDesc.getNameAsString();
    if (isNotIndexedTableDescriptor(tableDesc)) {
      return;
    }
    LOG.trace("Entering preRollBack for the table " + userTableName + " for the region "
        + region.getRegionInfo());
    SplitInfo splitInfo = splitThreadLocal.get();
    if (splitInfo == null) return;
    IndexSplitTransaction splitTransaction = splitInfo.getSplitTransaction();
    try {
      if (splitTransaction != null) {
        splitTransaction.rollback(rs, rs);
        LOG.info("preRollBack successfully done for the table " + userTableName
            + " for the region " + region.getRegionInfo());
      }
    } catch (Exception e) {
      LOG.error(
        "Error while rolling back the split failure for index region " + splitInfo.getParent(), e);
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

  public synchronized static void addSeekPoints(List<byte[]> seekPoints) {
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
      InternalScanner s, CompactionRequest request) throws IOException {
    HRegionServer rs = (HRegionServer) c.getEnvironment().getRegionServerServices();
    if (!IndexUtils.isIndexTable(store.getTableName())) {
      // Not an index table
      return null;
    }
    long smallestReadPoint = c.getEnvironment().getRegion().getSmallestReadPoint();
    String actualTableName = IndexUtils.getActualTableName(store.getTableName().getNameAsString());
    TTLStoreScanner ttlStoreScanner =
        new TTLStoreScanner(store, smallestReadPoint, earliestPutTs, scanType, scanners,
            new TTLExpiryChecker(), actualTableName, rs);
    return ttlStoreScanner;
  }

  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
    HRegion region = e.getEnvironment().getRegion();
    byte[] tableName = region.getRegionInfo().getTable().getName();
    if (IndexUtils.isCatalogOrSystemTable(region.getTableDesc().getTableName())
        || IndexUtils.isIndexTable(tableName)) {
      return;
    }
    if (splitThreadLocal.get() == null) {
      this.indexManager.decrementRegionCount(Bytes.toString(tableName), true);
    } else {
      this.indexManager.decrementRegionCount(Bytes.toString(tableName), false);
    }
  }

  private boolean isValidIndexMutation(HTableDescriptor userTableDesc) {
    String tableName = userTableDesc.getTableName().getNameAsString();
    if (IndexUtils.isCatalogOrSystemTable(userTableDesc.getTableName())
        || IndexUtils.isIndexTable(tableName)) {
      return false;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName);
    if (indices == null || indices.isEmpty()) {
      LOG.trace("skipping preBatchMutate for the table " + tableName + " as there are no indices");
      return false;
    }
    return true;
  }

  private void acquireLockOnIndexRegion(String tableName, HRegion userRegion, HRegionServer rs,
      Operation op) throws IOException {
    HRegion indexRegion = getIndexTableRegion(tableName, userRegion, rs);
    indexRegion.checkResources();
    indexRegion.startRegionOperation(op);
  }

  @Override
  public void postCloseRegionOperation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Operation op) throws IOException {
    if (op.equals(Operation.BATCH_MUTATE)) {
      HRegionServer rs = (HRegionServer) ctx.getEnvironment().getRegionServerServices();
      HRegion userRegion = ctx.getEnvironment().getRegion();
      HTableDescriptor userTableDesc = userRegion.getTableDesc();
      String tableName = userTableDesc.getNameAsString();
      if (isNotIndexedTableDescriptor(userTableDesc)) {
        return;
      }
      if (!isValidIndexMutation(userTableDesc)) {
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
  }

  @Override
  public void
      postStartRegionOperation(ObserverContext<RegionCoprocessorEnvironment> e, Operation op)
          throws IOException {
    if (op.equals(Operation.BATCH_MUTATE)) {
      HRegionServer rs = (HRegionServer) e.getEnvironment().getRegionServerServices();
      HRegion userRegion = e.getEnvironment().getRegion();
      HTableDescriptor userTableDesc = userRegion.getTableDesc();
      String tableName = userTableDesc.getNameAsString();
      if (isNotIndexedTableDescriptor(userTableDesc)) {
        return;
      }
      if (!isValidIndexMutation(userTableDesc)) {
        return;
      }
      acquireLockOnIndexRegion(tableName, userRegion, rs, op);
    }
  }

  @Override
  public Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
      FileSystem fs, Path path, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
      Reference r, Reader reader) throws IOException {
    Configuration conf = ctx.getEnvironment().getConfiguration();
    if (reader == null && r != null && isIndexRegionReference(path)) {
      return new IndexHalfStoreFileReader(fs, path, cacheConf, in, size, r, conf);
    }
    return reader;
  }

  private boolean isIndexRegionReference(Path path) {
    String tablePath = path.getParent().getParent().getParent().getName();
    return tablePath.endsWith(Constants.INDEX_TABLE_SUFFIX);
  }

  /**
   * Information of dependent region split processed in coprocessor hook
   * {@link RegionCoprocessorHost#preSplitBeforePONR(byte[], List)}
   */
  public static class SplitInfo {
    private PairOfSameType<HRegion> daughterRegions;
    private IndexSplitTransaction st;
    private HRegion parent;

    public SplitInfo(final HRegion parent, final PairOfSameType<HRegion> pairOfSameType,
        final IndexSplitTransaction st) {
      this.parent = parent;
      this.daughterRegions = pairOfSameType;
      this.st = st;
    }

    public PairOfSameType<HRegion> getDaughters() {
      return this.daughterRegions;
    }

    public IndexSplitTransaction getSplitTransaction() {
      return this.st;
    }

    public HRegion getParent() {
      return this.parent;
    }
  }

}
