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
package org.apache.hadoop.hbase.index.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.exception.StaleRegionBoundaryException;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ServiceException;

/**
 * Extension of HBaseAdmin to perform index related operations. This also should be used when we
 * create table with index details, and other admin operations on indexed table.
 */
public class IndexAdmin extends HBaseAdmin {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  private final int numRetries;
  private final long pause;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Multiply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;

  
  public IndexAdmin(Configuration c) throws IOException {
    super(c);
    this.pause = c.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.retryLongerMultiplier = c.getInt(
      "hbase.client.retries.longer.multiplier", 10);

  }

  public IndexAdmin(HConnection connection) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    super(connection);
    this.pause = getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.retryLongerMultiplier = getConfiguration().getInt(
      "hbase.client.retries.longer.multiplier", 10);
  }

  @Override
  public void createTable(final HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    try {
      createTableAsync(desc, splitKeys);
    } catch (SocketTimeoutException ste) {
      LOG.warn("Creating " + desc.getNameAsString() + " took too long", ste);
    }
    int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
    int prevRegCount = 0;

    MetaScannerVisitorBaseWithTableName userTableVisitor = null;
    MetaScannerVisitorBaseWithTableName indexTableVisitor = null;
    boolean indexedHTD = desc.getValue(Constants.INDEX_SPEC_KEY) != null;

    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {

      AtomicInteger actualRegCount = null;
      // Wait for new table to come on-line
      if (userTableVisitor == null) {
        userTableVisitor = new MetaScannerVisitorBaseWithTableName(desc.getNameAsString());
      }
      actualRegCount = userTableVisitor.getActualRgnCnt();
      actualRegCount.set(0);
      MetaScanner.metaScan(getConfiguration(), getConnection(), userTableVisitor,
        desc.getTableName());
      if (actualRegCount.get() != numRegs) {
        if (tries == this.numRetries * this.retryLongerMultiplier - 1) {
          throw new RegionOfflineException("Only " + actualRegCount.get() + " of " + numRegs
              + " regions are online; retries exhausted.");
        }
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when opening" + " regions; "
              + actualRegCount.get() + " of " + numRegs + " regions processed so far");
        }
        if (actualRegCount.get() > prevRegCount) { // Making progress
          prevRegCount = actualRegCount.get();
          tries = -1;
        }
      } else {
        if (indexedHTD) {
          TableName indexTableName =
              TableName.valueOf(IndexUtils.getIndexTableName(desc.getName()));
          if (indexTableVisitor == null) {
            indexTableVisitor =
                new MetaScannerVisitorBaseWithTableName(indexTableName.getNameAsString());
          }
          actualRegCount = indexTableVisitor.getActualRgnCnt();
          actualRegCount.set(0);
          MetaScanner.metaScan(getConfiguration(), getConnection(), indexTableVisitor,
            indexTableName);
          if (actualRegCount.get() != numRegs) {
            if (tries == this.numRetries * this.retryLongerMultiplier - 1) {
              throw new RegionOfflineException("Only " + actualRegCount.get() + " of " + numRegs
                  + " regions are online; retries exhausted.");
            }
            try { // Sleep
              Thread.sleep(getPauseTime(tries));
            } catch (InterruptedException e) {
              throw new InterruptedIOException("Interrupted when opening" + " regions; "
                  + actualRegCount.get() + " of " + numRegs + " regions processed so far");
            }
            if (actualRegCount.get() > prevRegCount) { // Making progress
              prevRegCount = actualRegCount.get();
              tries = -1;
            }
          } else if (isTableEnabled(indexTableName)) {
            return;
          }
        } else if (isTableEnabled(desc.getName())) {
          return;
        }
      }
    }
    throw new TableNotEnabledException("Retries exhausted while still waiting for table: "
        + desc.getNameAsString() + " to be enabled");
  }

  static class MetaScannerVisitorBaseWithTableName implements MetaScannerVisitor {
    byte[] tableName = null;
    AtomicInteger actualRegCount = new AtomicInteger(0);

    MetaScannerVisitorBaseWithTableName(String tableName) {
      this.tableName = Bytes.toBytes(tableName);
    }

    AtomicInteger getActualRgnCnt() {
      return actualRegCount;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean processRow(Result rowResult) throws IOException {
      HRegionInfo info = HRegionInfo.getHRegionInfo(rowResult);
      // If regioninfo is null, skip this row
      if (info == null) {
        return true;
      }
      if (!(Bytes.equals(info.getTable().getName(), tableName))) {
        return false;
      }
      ServerName serverName = HRegionInfo.getServerName(rowResult);
      // Make sure that regions are assigned to server
      if (!(info.isOffline() || info.isSplit()) && serverName != null
          && serverName.getHostAndPort() != null) {
        actualRegCount.incrementAndGet();
      }
      return true;
    }
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    boolean indexEnabled = getConfiguration().getBoolean("hbase.use.secondary.index", false);
    if (!indexEnabled) {
      return getConnection().isTableEnabled(tableName);
    } else {
      boolean isTableEnabled = getConnection().isTableEnabled(tableName);
      if (isTableEnabled && !IndexUtils.isIndexTable(tableName.getNameAsString())) {
        TableName indexTableName =
            TableName.valueOf(IndexUtils.getIndexTableName(tableName.getNameAsString()));
        if (getConnection().isTableAvailable(indexTableName)) {
          return getConnection().isTableEnabled(indexTableName);
        }
        return true;
      }
      return isTableEnabled;
    }
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
    boolean indexEnabled = getConfiguration().getBoolean("hbase.use.secondary.index", false);
    if (!indexEnabled) {
      return getConnection().isTableDisabled(tableName);
    } else {
      boolean isTableDisabled = getConnection().isTableDisabled(tableName);
      if (isTableDisabled && !IndexUtils.isIndexTable(tableName.getNameAsString())) {
        TableName indexTableName =
            TableName.valueOf(IndexUtils.getIndexTableName(tableName.getNameAsString()));
        if (getConnection().isTableAvailable(indexTableName)) {
          return getConnection().isTableDisabled(indexTableName);
        }
        return true;
      }
      return isTableDisabled;
    }
  }
  
  @Override
  public void deleteTable(TableName tableName) throws IOException {
    super.deleteTable(tableName);
    boolean indexEnabled = getConfiguration().getBoolean("hbase.use.secondary.index", false);
    if (indexEnabled && !IndexUtils.isIndexTable(tableName.getNameAsString())) {
      TableName indexTableName =
          TableName.valueOf(IndexUtils.getIndexTableName(tableName.getNameAsString()));
      if (getConnection().isTableDisabled(indexTableName)) {
        waitUntilTableIsDeleted(indexTableName);
      }
      
    } 
  }
  
  /**
   * Wait until table is deleted. 
   * @param tableName
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public void waitUntilTableIsDeleted(final TableName tableName) throws IOException {
    boolean tableExists = true;
    int failures = 0;
    // Wait until all regions deleted
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {
        HRegionLocation firstMetaServer = getConnection().locateRegion(TableName.META_TABLE_NAME,
          HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
        Scan scan = MetaReader.getScanForTableName(tableName);
        scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        ScanRequest request = RequestConverter.buildScanRequest(
          firstMetaServer.getRegionInfo().getRegionName(), scan, 1, true);
        Result[] values = null;
        // Get a batch at a time.
        ClientService.BlockingInterface server = getConnection().getClient(firstMetaServer
            .getServerName());
        PayloadCarryingRpcController controller = new PayloadCarryingRpcController();
        try {
          controller.setPriority(tableName);
          ScanResponse response = server.scan(controller, request);
          values = ResponseConverter.getResults(controller.cellScanner(), response);
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }

        // let us wait until hbase:meta table is updated and
        // HMaster removes the table from its HTableDescriptors
        if (values == null || values.length == 0) {
          try {
            getTableDescriptor(tableName);
          } catch (TableNotFoundException t) {
            tableExists = false;
            break;
          }
        }
      } catch (IOException ex) {
        failures++;
        if(failures == numRetries - 1) {           // no more tries left
          if (ex instanceof RemoteException) {
            throw ((RemoteException) ex).unwrapRemoteException();
          } else {
            throw ex;
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }

    if (tableExists) {
      throw new IOException("Retries exhausted, it took too long to wait"+
        " for the table " + tableName + " to be deleted.");
    }
    // Delete cached information to prevent clients from using old locations
    this.getConnection().clearRegionCache(tableName);
    LOG.info("Deleted " + tableName);
  }
  

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }
  
  // TODO add APIs to have table name of string type and bytes type.
  /**
   * Add specified index to the specified table.
   * @param tableName
   * @param spec
   * @throws IOException 
   */
  public void addIndex(TableName tableName, IndexSpecification spec) throws IOException {
    if (tableName == null || spec == null) {
      throw new IllegalArgumentException("TableName or index details should not be null.");
    }
    List<IndexSpecification> specs = new ArrayList<IndexSpecification>(1);
    specs.add(spec);
    addIndexes(tableName, specs);
  }
  
  /**
   * Add specified indexes to the specified table.
   * @param tableName
   * @param specs
   * @throws IOException 
   */
  public void addIndexes(TableName tableName, List<IndexSpecification> specs) throws IOException {
    if (tableName == null || specs == null || specs.isEmpty()) {
      throw new IllegalArgumentException("TableName or index details should not be null or empty.");
    }
    if (!getConfiguration().getBoolean("hbase.use.secondary.index", false)) {
      throw new IllegalArgumentException(
          "Secondary index not enabled. Configure hbase.use.secondary.index"
              + "to true and verify at server as well.");
    }
    HTableDescriptor descriptor = getTableDescriptor(tableName);
    byte[] indexBytes = descriptor.getValue(Constants.INDEX_SPEC_KEY);
    // TODO: Validate indexes like duplicate indexes and index column families and all the things..
    // check validations in preCreateTable hook impl.
    if (indexBytes == null) {
      TableIndices ti = new TableIndices();
      ti.addIndexes(specs);
      byte[] byteArray = ti.toByteArray();
      Map<Column, Pair<ValueType, Integer>> indexColDetails =
          new HashMap<Column, Pair<ValueType, Integer>>();
      for (IndexSpecification spec : specs) {
        IndexUtils.checkColumnsForValidityAndConsistency(descriptor, spec, indexColDetails);
      }
      descriptor.setValue(Constants.INDEX_SPEC_KEY, byteArray);
      disableTable(tableName);
      modifyTable(tableName, descriptor);
      enableTable(tableName);
      buildIndexes(tableName, ti);
    } else {
      // TODO: add tests for this.
      TableIndices ti = new TableIndices();
      ti.readFields(indexBytes);
      ti.addIndexes(specs);
      Map<Column, Pair<ValueType, Integer>> indexColDetails =
          new HashMap<Column, Pair<ValueType, Integer>>();
      for (IndexSpecification spec : ti.getIndices()) {
        IndexUtils.checkColumnsForValidityAndConsistency(descriptor, spec, indexColDetails);
      }
      byte[] byteArray = ti.toByteArray();
      descriptor.setValue(Constants.INDEX_SPEC_KEY, byteArray);
      disableTable(tableName);
      modifyTable(tableName, descriptor);
      enableTable(tableName);
      TableIndices newIndices = new TableIndices();
      newIndices.addIndexes(specs);
      buildIndexes(tableName, newIndices);
    }
  }
  
  private void buildIndexes(TableName tableName, TableIndices newIndices) throws IOException {
    HTable table = null;
    try{
      table = new HTable(getConfiguration(), tableName);
      Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
      byte[][] startKeys = startEndKeys.getFirst();
      byte[][] endKeys = startEndKeys.getSecond();
      for (int i = 0; i < startKeys.length; i++) {
        List<Pair<byte[], byte[]>> keyRanges = new ArrayList<Pair<byte[],byte[]>>(1);
        keyRanges.add(new Pair<byte[], byte[]>(startKeys[i], endKeys[i]));
        Scan commonScan = new Scan();
        List<IndexSpecification> indices = newIndices.getIndices();
        for (int j = 0; j < indices.size(); j++) {
          IndexSpecification indexSpecification = indices.get(j);
          Iterator<ColumnQualifier> itr = indexSpecification.getIndexColumns().iterator();
          while (itr.hasNext()) {
            ColumnQualifier columnQualifier = itr.next();
            commonScan.addColumn(columnQualifier.getColumnFamily(), columnQualifier.getQualifier());
          }
        }
        commonScan.setAttribute(Constants.BUILD_INDICES, newIndices.toByteArray());
        openRegionScanner(table, keyRanges, commonScan);
      }
    } finally {
      if (table != null) table.close();
    }
  }

  // TODO add APIs to have table name of string type and bytes type.
  
  /**
   * Drop specified index to the specified table.
   * @param tableName
   * @param indexName
   * @throws IOException 
   * @throws TableNotFoundException 
   */
  public void dropIndex(TableName tableName, String indexName) throws TableNotFoundException,
      IOException {
    if (tableName == null || indexName == null) {
      // TODO: add proper error message
      throw new IllegalArgumentException();
    }
    List<String> indexes = new ArrayList<String>(1);
    indexes.add(indexName);
    dropIndexes(tableName, indexes);
  }
  
  /**
   * Drop specified indexes to the specified table.
   * @param tableName
   * @param indexNames
   * @throws IOException 
   * @throws TableNotFoundException 
   */
  public void dropIndexes(TableName tableName, List<String> indexNames)
      throws TableNotFoundException, IOException {
    if (tableName == null || indexNames == null || indexNames.isEmpty()) {
      throw new IllegalArgumentException("Table name is null or indexes empty or null.");
    }
    if (!getConfiguration().getBoolean("hbase.use.secondary.index", false)) {
      throw new IllegalArgumentException(
          "Secondary index not enabled. Configure hbase.use.secondary.index"
              + "to true and verify at server as well.");
    }
    HTableDescriptor tableDesc = getTableDescriptor(tableName);
    byte[] indexBytes = tableDesc.getValue(Constants.INDEX_SPEC_KEY);
    if (indexBytes == null) throw new IllegalArgumentException(tableName
        + " is not an indexed to drop the indices.");
    TableIndices ti = new TableIndices();
    ti.readFields(indexBytes);
    List<IndexSpecification> indices = ti.getIndices();
    TableIndices indicesToDrop = new TableIndices();
    TableIndices indicesLeft = new TableIndices();
    for (IndexSpecification spec : indices) {
      if (indexNames.contains(spec.getName())) {
        indicesToDrop.addIndex(spec);
        indexNames.remove(spec.getName());
      } else {
        indicesLeft.addIndex(spec);
      }
    }
    if (!indexNames.isEmpty()) throw new IllegalArgumentException("Index(es) " + indexNames
        + " not found in the table indices list.Specify valid index names.");
    if(indicesLeft.getIndices().isEmpty()) {
      tableDesc.remove(Constants.INDEX_SPEC_KEY);
    } else {
      tableDesc.setValue(Constants.INDEX_SPEC_KEY, indicesLeft.toByteArray());
    }
    disableTable(tableName);
    modifyTable(tableName, tableDesc);
    enableTable(tableName);
    if (tableExists(IndexUtils.getIndexTableName(tableName))) {
      HTable table = null;
      try {
        table = new HTable(getConfiguration(), tableName);
        Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for(int i = 0; i< startKeys.length; i++) {
          List<Pair<byte[], byte[]>> keyRanges = new ArrayList<Pair<byte[],byte[]>>(1);
          keyRanges.add(new Pair<byte[], byte[]>(startKeys[i], endKeys[i]));
          Scan commonScan = new Scan();
          commonScan.setAttribute(Constants.DROP_INDICES, indicesToDrop.toByteArray());
          openRegionScanner(table, keyRanges, commonScan);
        }
      } finally {
        if (table != null) table.close();
      }
    }
  }

  private ResultScanner openRegionScanner(HTable table, List<Pair<byte[], byte[]>> keyRanges,
      Scan scan) throws IOException {
    ResultScanner scanner = null;
    Iterator<Pair<byte[], byte[]>> iterator = keyRanges.iterator();
    while (iterator.hasNext()) {
      Pair<byte[], byte[]> pair = iterator.next();
      iterator.remove();
      Scan s = new Scan(scan);
      s.setStartRow(pair.getFirst());
      s.setStopRow(pair.getSecond());
      try {
        scanner = table.getScanner(s);
      } catch (StaleRegionBoundaryException srbe) {
        LOG.debug(srbe);
        table.clearRegionCache();
        Pair<byte[][], byte[][]> splits = table.getStartEndKeys();
        for(int j = 0; j< splits.getFirst().length; j++) {
          if ((Bytes.compareTo(pair.getFirst(), splits.getFirst()[j]) == 0 && Bytes.compareTo(
            pair.getSecond(), splits.getSecond()[j]) > 0)
              || (Bytes.compareTo(pair.getFirst(), splits.getFirst()[j]) < 0 && Bytes
                  .compareTo(pair.getSecond(), splits.getSecond()[j]) == 0)
              || (Bytes.compareTo(pair.getFirst(), splits.getFirst()[j]) < 0 && Bytes
                  .compareTo(pair.getSecond(), splits.getSecond()[j]) > 0)) {
            keyRanges
                .add(new Pair<byte[], byte[]>(splits.getFirst()[j], splits.getSecond()[j]));
          }
        }
        iterator = keyRanges.iterator();
      } finally {
        if (scanner != null) {
          scanner.close();
          scanner = null;
        }
      }
    }
    return scanner;
  }
}
