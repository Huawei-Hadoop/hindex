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
package org.apache.hadoop.hbase.index.manager;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexManager manages the index details of each table.
 */
public class IndexManager {

  // manager is Singleton object
  private static IndexManager manager = new IndexManager();

  private Map<String, List<IndexSpecification>> tableVsIndices =
      new ConcurrentHashMap<String, List<IndexSpecification>>();

  private ConcurrentHashMap<String, AtomicInteger> tableVsNumberOfRegions =
      new ConcurrentHashMap<String, AtomicInteger>();

  // TODO one DS is enough
  private Map<String, Map<byte[], IndexSpecification>> tableIndexMap =
      new ConcurrentHashMap<String, Map<byte[], IndexSpecification>>();

  private IndexManager() {

  }

  /**
   * @return IndexManager instance
   */
  public static IndexManager getInstance() {
    return manager;
  }

  /**
   * @param tableName on which index applying
   * @param indexList list of table
   */
  public void addIndexForTable(String tableName, List<IndexSpecification> indexList) {
    this.tableVsIndices.put(tableName, indexList);
    // TODO the inner map needs to be thread safe when we support dynamic index add/remove
    Map<byte[], IndexSpecification> indexMap =
        new TreeMap<byte[], IndexSpecification>(Bytes.BYTES_COMPARATOR);
    for (IndexSpecification index : indexList) {
      ByteArrayBuilder keyBuilder = ByteArrayBuilder.allocate(IndexUtils.getMaxIndexNameLength());
      keyBuilder.put(Bytes.toBytes(index.getName()));
      indexMap.put(keyBuilder.array(), index);
    }
    this.tableIndexMap.put(tableName, indexMap);
  }

  /**
   * @param tableName on which index applying
   * @return IndexSpecification list for the table or return null if no index for the table
   */
  public List<IndexSpecification> getIndicesForTable(String tableName) {
    return this.tableVsIndices.get(tableName);
  }

  /**
   * @param tableName on which index applying
   */
  public void removeIndices(String tableName) {
    this.tableVsIndices.remove(tableName);
    this.tableIndexMap.remove(tableName);
  }

  /**
   * @param tableName
   * @param indexName
   * @return index specification
   */
  public IndexSpecification getIndex(String tableName, byte[] indexName) {
    Map<byte[], IndexSpecification> indices = this.tableIndexMap.get(tableName);
    if (indices != null) {
      return indices.get(indexName);
    }
    return null;
  }

  public void incrementRegionCount(String tableName) {
    AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
    // Here synchronization is needed for the first time count operation to be
    // initialized
    if (null == count) {
      synchronized (tableVsNumberOfRegions) {
        count = this.tableVsNumberOfRegions.get(tableName);
        if (null == count) {
          count = new AtomicInteger(0);
          this.tableVsNumberOfRegions.put(tableName, count);
        }
      }
    }
    count.incrementAndGet();

  }

  public void decrementRegionCount(String tableName, boolean removeIndices) {
    // Need not be synchronized here because anyway the decrement operator
    // will work atomically. Ultimately atleast one thread will see the count
    // to be 0 which should be sufficient to remove the indices
    AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
    if (null != count) {
      int next = count.decrementAndGet();
      if (next == 0) {
        this.tableVsNumberOfRegions.remove(tableName);
        if (removeIndices) {
          this.removeIndices(tableName);
        }
      }
    }
  }

  // API needed for test cases.
  public int getTableRegionCount(String tableName) {
    AtomicInteger count = this.tableVsNumberOfRegions.get(tableName);
    if (count != null) {
      return count.get();
    }
    return 0;
  }
}
