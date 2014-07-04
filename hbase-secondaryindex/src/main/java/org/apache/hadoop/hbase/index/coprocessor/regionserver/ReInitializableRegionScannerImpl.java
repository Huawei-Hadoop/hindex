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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

// TODO better name?
public class ReInitializableRegionScannerImpl implements ReInitializableRegionScanner {

  private RegionScanner delegator;

  private int batch;

  private byte[] lastSeekedRowKey;

  private byte[] lastSeekAttemptedRowKey;

  private static final Log LOG = LogFactory.getLog(ReInitializableRegionScannerImpl.class);

  // Can this be queue?
  // Criteria for the selection to be additional heap overhead wrt the object
  // used cost in operation
  private Set<byte[]> seekPoints;

  private SeekPointFetcher seekPointFetcher;

  private boolean closed = false;

  public ReInitializableRegionScannerImpl(RegionScanner delegator, Scan scan,
      SeekPointFetcher seekPointFetcher) {
    this.delegator = delegator;
    this.batch = scan.getBatch();
    this.seekPoints = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    this.seekPointFetcher = seekPointFetcher;
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return this.delegator.getRegionInfo();
  }

  @Override
  public boolean isFilterDone() throws IOException {
    return this.delegator.isFilterDone();
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      this.delegator.close();
    } finally {
      this.seekPointFetcher.close();
    }
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void addSeekPoints(List<byte[]> seekPoints) {
    // well this add will do the sorting and remove duplicates. :)
    for (byte[] seekPoint : seekPoints) {
      this.seekPoints.add(seekPoint);
    }
  }

  @Override
  public boolean seekToNextPoint() throws IOException {
    // At this class level if seek is called it must be forward seek.
    // call reseek() directly with the next seek point
    Iterator<byte[]> spIterator = this.seekPoints.iterator();
    if (spIterator.hasNext()) {
      this.lastSeekAttemptedRowKey = spIterator.next();
      if (null != this.lastSeekedRowKey
          && Bytes.BYTES_COMPARATOR.compare(this.lastSeekedRowKey, this.lastSeekAttemptedRowKey) > 0) {
        throw new SeekUnderValueException();
      }
      spIterator.remove();
      LOG.trace("Next seek point " + Bytes.toString(this.lastSeekAttemptedRowKey));
      boolean reseekResult = closed ? false : this.reseek(this.lastSeekAttemptedRowKey);
      if (!reseekResult) return false;
      this.lastSeekedRowKey = this.lastSeekAttemptedRowKey;
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean reseek(byte[] row) throws IOException {
    return this.delegator.reseek(row);
  }

  @Override
  public void reInit(RegionScanner rs) throws IOException {
    this.delegator.close();
    this.delegator = rs;
    this.lastSeekedRowKey = null;
    this.lastSeekAttemptedRowKey = null;
    this.closed = false;
  }

  @Override
  public byte[] getLatestSeekpoint() {
    return this.lastSeekAttemptedRowKey;
  }

  @Override
  public long getMvccReadPoint() {
    return this.delegator.getMvccReadPoint();
  }

  @Override
  public boolean nextRaw(List<Cell> result) throws IOException {
    return this.delegator.nextRaw(result);
  }

  @Override
  public boolean nextRaw(List<Cell> result, int limit) throws IOException {
    return this.delegator.nextRaw(result, limit);
  }

  @Override
  public boolean next(List<Cell> results) throws IOException {
    return next(results, this.batch);
  }

  @Override
  public boolean next(List<Cell> result, int limit) throws IOException {
    // Before every next call seek to the appropriate position.
    if (closed) return false;
    while (!seekToNextPoint()) {
      List<byte[]> seekPoints = new ArrayList<byte[]>(1);
      // TODO Do we need to fetch more seekpoints here?
      if (!closed) this.seekPointFetcher.nextSeekPoints(seekPoints, 1);
      if (seekPoints.isEmpty()) {
        // nothing further to fetch from the index table.
        return false;
      }
      this.seekPoints.addAll(seekPoints);
    }
    return closed ? false : this.delegator.next(result, limit);
  }

  @Override
  public long getMaxResultSize() {
    return this.delegator.getMaxResultSize();
  }
}
