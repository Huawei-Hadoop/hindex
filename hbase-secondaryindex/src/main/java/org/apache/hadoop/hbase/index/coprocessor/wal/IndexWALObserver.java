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
package org.apache.hadoop.hbase.index.coprocessor.wal;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
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

public class IndexWALObserver implements WALObserver {

  private static final Log LOG = LogFactory.getLog(IndexWALObserver.class);

  private IndexManager indexManager = IndexManager.getInstance();

  @Override
  public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> ctx, HRegionInfo info,
      HLogKey logKey, WALEdit logEdit) throws IOException {
    TableName tableName = info.getTable();
    if (IndexUtils.isCatalogOrSystemTable(tableName) || IndexUtils.isIndexTable(tableName)) {
      return true;
    }
    List<IndexSpecification> indices = indexManager.getIndicesForTable(tableName.getNameAsString());
    if (indices != null && !indices.isEmpty()) {
      LOG.trace("Entering preWALWrite for the table " + tableName);
      String indexTableName = IndexUtils.getIndexTableName(tableName);
      IndexEdits iEdits = IndexRegionObserver.threadLocal.get();
      WALEdit indexWALEdit = iEdits.getWALEdit();
      // This size will be 0 when none of the Mutations to the user table to be indexed.
      // or write to WAL is disabled for the Mutations
      if (indexWALEdit.getKeyValues().size() == 0) {
        return true;
      }
      LOG.trace("Adding indexWALEdits into WAL for table " + tableName);
      HRegion indexRegion = iEdits.getRegion();
      // TS in all KVs within WALEdit will be the same. So considering the 1st one.
      Long time = indexWALEdit.getKeyValues().get(0).getTimestamp();
      indexRegion.getLog().appendNoSync(indexRegion.getRegionInfo(),
        TableName.valueOf(indexTableName), indexWALEdit, logKey.getClusterIds(), time,
        indexRegion.getTableDesc(), indexRegion.getSequenceId(), true, HConstants.NO_NONCE,
        HConstants.NO_NONCE);
      LOG.trace("Exiting preWALWrite for the table " + tableName);
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

}
