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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class BackwardSeekableRegionScanner implements SeekAndReadRegionScanner {

  private ReInitializableRegionScanner delegator;

  private Scan scan;

  private HRegion hRegion;

  private byte[] startRow;

  private boolean closed = false;

  public BackwardSeekableRegionScanner(ReInitializableRegionScanner delegator, Scan scan,
      HRegion hRegion, byte[] startRow) {
    this.delegator = delegator;
    this.scan = scan;
    this.hRegion = hRegion;
    this.startRow = startRow;
  }

  Scan getScan() {
    return scan;
  }

  byte[] getStartRow() {
    return startRow;
  }

  // For testing.
  RegionScanner getDelegator() {
    return delegator;
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
    this.delegator.close();
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public synchronized boolean next(List<Cell> results) throws IOException {
    return next(results, this.scan.getBatch());
  }

  @Override
  public boolean next(List<Cell> result, int limit) throws IOException {
    boolean hasNext = false;
    try {
      if (this.delegator.isClosed()) return false;
      hasNext = this.delegator.next(result, limit);
    } catch (SeekUnderValueException e) {
      Scan newScan = new Scan(this.scan);
      // Start from the point where we got stopped because of seek backward
      newScan.setStartRow(getLatestSeekpoint());
      this.delegator.reInit(this.hRegion.getScanner(newScan));
      hasNext = next(result, limit);
    }
    return hasNext;
  }

  @Override
  public synchronized boolean reseek(byte[] row) throws IOException {
    return this.delegator.reseek(row);
  }

  @Override
  public void addSeekPoints(List<byte[]> seekPoints) {
    this.delegator.addSeekPoints(seekPoints);
  }

  @Override
  public boolean seekToNextPoint() throws IOException {
    return this.delegator.seekToNextPoint();
  }

  @Override
  public byte[] getLatestSeekpoint() {
    return this.delegator.getLatestSeekpoint();
  }

  @Override
  public long getMaxResultSize() {
    return this.delegator.getMaxResultSize();
  }

  @Override
  public long getMvccReadPoint() {
    return this.delegator.getMvccReadPoint();
  }

  @Override
  public boolean nextRaw(List<Cell> result) throws IOException {
    return next(result);
  }

  @Override
  public boolean nextRaw(List<Cell> result, int limit) throws IOException {
    return next(result, limit);
  }
}
