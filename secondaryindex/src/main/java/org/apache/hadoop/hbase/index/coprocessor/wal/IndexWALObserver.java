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
package org.apache.hadoop.hbase.index.coprocessor.wal;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver;
import org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver.IndexEdits;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexWALObserver implements WALObserver {

  private static final Log LOG = LogFactory.getLog(IndexWALObserver.class);

  private IndexManager indexManager = IndexManager.getInstance();

  @Override
  public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
    String tableNameStr = info.getTableNameAsString();
    if (IndexUtils.isCatalogTable(info.getTableName()) || IndexUtils.isIndexTable(tableNameStr)) {
      return true;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableNameStr);
    if (indices != null && !indices.isEmpty()) {
      LOG.trace("Entering preWALWrite for the table " + tableNameStr);
      String indexTableName = IndexUtils.getIndexTableName(tableNameStr);
      IndexEdits iEdits = IndexRegionObserver.threadLocal.get();
      WALEdit indexWALEdit = iEdits.getWALEdit();
      // This size will be 0 when none of the Mutations to the user table to be indexed.
      // or write to WAL is disabled for the Mutations
      if (indexWALEdit.getKeyValues().size() == 0) {
        return true;
      }
      LOG.trace("Adding indexWALEdits into WAL for table " + tableNameStr);
      HRegion indexRegion = iEdits.getRegion();
      // TS in all KVs within WALEdit will be the same. So considering the 1st one.
      Long time = indexWALEdit.getKeyValues().get(0).getTimestamp();
      ctx.getEnvironment()
          .getWAL()
          .appendNoSync(indexRegion.getRegionInfo(), Bytes.toBytes(indexTableName), indexWALEdit,
            logKey.getClusterId(), time, indexRegion.getTableDesc());
      LOG.trace("Exiting preWALWrite for the table " + tableNameStr);
    }
    return true;
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
  }

  /*
   * HLog log =ctx.getEnvironment().getWAL(); List<KeyValue> kvList = logEdit.getKeyValues(); String
   * userTableName = info.getEncodedName(); List<IndexSpecification> indices =
   * IndexManager.getInstance().getIndicesForTable(userTableName); if(null != indices){
   * handleWalEdit(userTableName, log, kvList, indices, info); } return false; } private void
   * handleWalEdit(String userTableName, HLog log, List<KeyValue> kvList, List<IndexSpecification>
   * indices, HRegionInfo info) { HashMap<byte[], Map<byte[], List<KeyValue>>> myMap = new
   * HashMap<byte[], Map<byte[], List<KeyValue>>>(); for (KeyValue kv : kvList) { for
   * (IndexSpecification idx : indices) { Set<ColumnQualifier> colSet = idx.getIndexColumns(); for
   * (ColumnQualifier col : colSet) { if (Bytes.equals(kv.getFamily(), col.getColumnFamily()) &&
   * Bytes.equals(kv.getQualifier(), col.getQualifier())) { Map<byte[], List<KeyValue>>
   * mapOfCfToListOfKV = myMap.get(kv.getRow()); if (mapOfCfToListOfKV == null) { mapOfCfToListOfKV
   * = new HashMap<byte[], List<KeyValue>>(); myMap.put(kv.getRow(), mapOfCfToListOfKV); } //
   * listOfKVs.add(kv); List<KeyValue> listOfKV = mapOfCfToListOfKV.get(idx.getName().getBytes());
   * if(listOfKV == null ){ listOfKV = new ArrayList<KeyValue>();
   * mapOfCfToListOfKV.put(idx.getName().getBytes(), listOfKV); } listOfKV.add(kv.clone()); } } } }
   * createIndexWalEdit(myMap, indices, info); } private void createIndexWalEdit( HashMap<byte[],
   * Map<byte[], List<KeyValue>>> myMap, List<IndexSpecification> indices, HRegionInfo info) { int
   * totalValueLength = 0; byte[] primaryRowKey = null; byte[] prStartKey = info.getStartKey(); for
   * (Map.Entry<byte[], Map<byte[], List<KeyValue>>> mainEntry : myMap .entrySet()) { Map<byte[],
   * List<KeyValue>> idxMap = mainEntry.getValue(); for(IndexSpecification index : indices){ byte[]
   * name = index.getName().getBytes(); if(null != idxMap.get(name)){ Set<ColumnQualifier> colSet =
   * index.getIndexColumns(); for (ColumnQualifier c : colSet) { totalValueLength = totalValueLength
   * + c.getMaxValueLength(); } primaryRowKey = mainEntry.getKey(); int rowLength =
   * prStartKey.length + name.length; rowLength += totalValueLength; rowLength +=
   * primaryRowKey.length; } } } }
   */
}
