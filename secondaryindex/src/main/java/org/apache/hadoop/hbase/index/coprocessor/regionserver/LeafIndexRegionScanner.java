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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class LeafIndexRegionScanner implements IndexRegionScanner {

  private static final Log LOG = LogFactory.getLog(LeafIndexRegionScanner.class);

  private RegionScanner delegator = null;

  private KeyValue currentKV = null;

  private boolean hadMore = true;

  private final IndexSpecification index;

  private boolean isRangeScanner = false;

  private TTLExpiryChecker ttlExpiryChecker;

  private int scannerIndex = -1;

  public LeafIndexRegionScanner(IndexSpecification index, RegionScanner delegator,
      TTLExpiryChecker ttlExpiryChecker) {
    this.delegator = delegator;
    this.index = index;
    this.ttlExpiryChecker = ttlExpiryChecker;
  }

  @Override
  public void advance() {
    this.currentKV = null;
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return this.delegator.getRegionInfo();
  }

  @Override
  public boolean isFilterDone() {
    return this.delegator.isFilterDone();
  }

  @Override
  public void setRangeFlag(boolean range) {
    isRangeScanner = range;
  }

  @Override
  public boolean isRange() {
    return this.isRangeScanner;
  }

  @Override
  public void setScannerIndex(int index) {
    scannerIndex = index;
  }

  @Override
  public int getScannerIndex() {
    return scannerIndex;
  }

  @Override
  public boolean hasChildScanners() {
    return false;
  }

  // TODO the passing row to be the full key in the index table.
  // The callee need to take care of this creation..
  @Override
  public synchronized boolean reseek(byte[] row) throws IOException {
    if (!hadMore) return false;
    byte[] targetRowKey = createRowKeyForReseek(row);
    return this.delegator.reseek(targetRowKey);
  }

  private byte[] createRowKeyForReseek(byte[] targetRow) {
    byte[] curRK = this.currentKV.getRow();
    byte[] curValue = this.currentKV.getValue();
    short actualTabRKOffset = Bytes.toShort(curValue, 2);
    byte[] newRowKey = new byte[actualTabRKOffset + targetRow.length];
    System.arraycopy(curRK, 0, newRowKey, 0, actualTabRKOffset);
    System.arraycopy(targetRow, 0, newRowKey, actualTabRKOffset, targetRow.length);
    return newRowKey;
  }

  @Override
  public synchronized void close() throws IOException {
    this.delegator.close();
  }

  @Override
  public synchronized boolean next(List<KeyValue> results) throws IOException {
    boolean hasMore = false;
    do {
      // this check here will prevent extra next call when in the previous
      // next last row was fetched and after that an advance was called on this
      // scanner. So instead of making a next call again we can return from here.
      if (!this.hadMore) return false;
      hasMore = this.delegator.next(results);
      if (results != null && results.size() > 0) {
        KeyValue kv = results.get(0);
        if (this.ttlExpiryChecker.checkIfTTLExpired(this.index.getTTL(), kv.getTimestamp())) {
          results.clear();
          LOG.info("The ttl has expired for the kv " + kv);
        } else {
          if (!isRangeScanner) {
            // This is need to reseek in case of EQUAL scanners.
            this.currentKV = kv;
            break;
          }
        }
      }
    } while (results.size() < 1 && hasMore);
    this.hadMore = hasMore;
    return hasMore;
  }

  @Override
  public boolean next(List<KeyValue> result, int limit) throws IOException {
    // We wont call this method at all.. As we have only CF:qualifier per row in index table.
    throw new UnsupportedOperationException("Use only next(List<KeyValue> results) method.");
  }

  @Override
  public long getMvccReadPoint() {
    // TODO Implement RegionScanner.getMvccReadPoint
    return 0;
  }

  @Override
  public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
    // TODO Implement RegionScanner.nextRaw
    return false;
  }

  @Override
  public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
    // TODO Implement RegionScanner.nextRaw
    return false;
  }

  @Override
  public boolean next(List<KeyValue> results, String metric) throws IOException {
    // TODO Implement InternalScanner.next
    return false;
  }

  @Override
  public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
    // TODO Implement InternalScanner.next
    return false;
  }

}
