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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TTLStoreScanner implements InternalScanner {
  private InternalScanner delegate;
  private Store store;
  private long smallestReadPoint;
  private long earliestTS;
  private ScanType type; // (This should be scan type)
  private static final Log LOG = LogFactory.getLog(TTLStoreScanner.class);

  private TTLExpiryChecker ttlExpiryChecker;
  private String actualTableName;
  private HRegionServer rs;
  private Boolean userRegionAvailable = null;

  public TTLStoreScanner(Store store, long smallestReadPoint, long earliestTS, ScanType type,
      List<? extends KeyValueScanner> scanners, TTLExpiryChecker ttlExpiryChecker,
      String actualTableName, HRegionServer rs) throws IOException {
    this.store = store;
    this.smallestReadPoint = smallestReadPoint;
    this.earliestTS = earliestTS;
    this.type = type;
    Scan scan = new Scan();
    scan.setMaxVersions(store.getFamily().getMaxVersions());
    delegate =
        new StoreScanner(store, store.getScanInfo(), scan, scanners, type, this.smallestReadPoint,
            this.earliestTS);
    this.ttlExpiryChecker = ttlExpiryChecker;
    this.actualTableName = actualTableName;
    this.rs = rs;
  }

  @Override
  public boolean next(List<Cell> results) throws IOException {
    return this.next(results, 1);
  }

  @Override
  public boolean next(List<Cell> result, int limit) throws IOException {
    boolean next = this.delegate.next(result, limit);
    // Ideally here i should get only one result(i.e) only one kv
    for (Iterator<Cell> iterator = result.iterator(); iterator.hasNext();) {
      KeyValue kv = (KeyValue) iterator.next();
      byte[] indexNameInBytes = formIndexNameFromKV(kv);
      // From the indexname get the TTL
      IndexSpecification index =
          IndexManager.getInstance().getIndex(this.actualTableName, indexNameInBytes);
      HRegionInfo hri = store.getRegionInfo();
      if (this.type == ScanType.COMPACT_DROP_DELETES) {
        if (this.userRegionAvailable == null) {
          this.userRegionAvailable = isUserTableRegionAvailable(hri, this.rs);
        }
        // If index is null probably index is been dropped through drop index
        // If user region not available it may be due to the reason that the user region has not yet
        // opened but
        // the index region has opened.
        // Its better not to avoid the kv here, and write it during this current compaction.
        // Anyway later compaction will avoid it. May lead to false positives but better than
        // data loss
        if (null == index && userRegionAvailable) {
          // Remove the dropped index from the results
          LOG.info("The index has been removed for the kv " + kv);
          iterator.remove();
          continue;
        }
      }
      if (index != null) {
        boolean ttlExpired =
            this.ttlExpiryChecker.checkIfTTLExpired(index.getTTL(), kv.getTimestamp());
        if (ttlExpired) {
          result.clear();
          LOG.info("The ttl has expired for the kv " + kv);
          return false;
        }
      }
    }
    return next;
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
  }

  private byte[] formIndexNameFromKV(KeyValue kv) {
    byte[] rowKey = kv.getRow();
    // First two bytes are going to be the
    ByteArrayBuilder keyBuilder = ByteArrayBuilder.allocate(rowKey.length);
    // Start from 2nd offset because the first 2 bytes corresponds to the rowkeylength
    keyBuilder.put(rowKey, 0, rowKey.length);
    String replacedKey = Bytes.toString(keyBuilder.array());
    String emptyByte = Bytes.toString(new byte[1]);
    int indexOf = replacedKey.indexOf(emptyByte);
    return keyBuilder.array(indexOf + 1, IndexUtils.getMaxIndexNameLength());
  }

  private boolean isUserTableRegionAvailable(HRegionInfo indexHri, HRegionServer rs) {
    Collection<HRegion> userRegions = rs.getOnlineRegions(TableName.valueOf(this.actualTableName));
    for (HRegion userRegion : userRegions) {
      // TODO start key check is enough? May be we can check for the
      // possibility for N-1 Mapping?
      if (Bytes.equals(userRegion.getStartKey(), indexHri.getStartKey())) {
        return true;
      }
    }
    return false;
  }
}
