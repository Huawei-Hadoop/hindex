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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class IndexRegionScannerForAND implements IndexRegionScanner {

  private static final Log LOG = LogFactory.getLog(IndexRegionScannerForAND.class);

  private List<IndexRegionScanner> scanners = null;

  private Map<String, Pair<List<Boolean>, Integer>> rowCache =
      new HashMap<String, Pair<List<Boolean>, Integer>>();

  private int scannersCount = 0;

  volatile boolean hasRangeScanners = false;

  private int scannerIndex = -1;

  public IndexRegionScannerForAND(List<IndexRegionScanner> scanners) {
    this.scanners = scanners;
    scannersCount = scanners.size();
  }

  @Override
  public void advance() {
    for (IndexRegionScanner scn : this.scanners) {
      scn.advance();
    }
  }

  @Override
  public HRegionInfo getRegionInfo() {
    return null;
  }

  @Override
  public boolean isFilterDone() {
    return false;
  }

  @Override
  public boolean hasChildScanners() {
    return scannersCount != 0;
  };

  @Override
  public void setRangeFlag(boolean range) {
    hasRangeScanners = range;
  }

  @Override
  public boolean isRange() {
    return this.hasRangeScanners;
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
  public synchronized boolean reseek(byte[] row) throws IOException {
    // ideally reseek on AND may not come as AND cannot be a child of another AND.
    if (this.scanners.isEmpty()) return false;
    for (IndexRegionScanner scn : this.scanners) {
      boolean reseek = scn.reseek(row);
      if (!reseek) return false;
    }
    return true;
  }

  @Override
  public synchronized void close() throws IOException {
    for (IndexRegionScanner scn : this.scanners) {
      scn.close();
    }
    this.scanners.clear();
  }

  @Override
  public synchronized boolean next(List<Cell> results) throws IOException {
    if (this.scanners != null && !this.scanners.isEmpty()) {
      List<Pair<byte[], IndexRegionScanner>> valueList =
          new ArrayList<Pair<byte[], IndexRegionScanner>>();
      byte[] maxRowKey = null;
      while (results.size() < 1) {
        List<Cell> intermediateResult = new ArrayList<Cell>();
        boolean haveSameRows = true;
        Cell kv = null;
        Iterator<IndexRegionScanner> scnItr = this.scanners.iterator();
        while (scnItr.hasNext()) {
          IndexRegionScanner scn = scnItr.next();
          if (!hasRangeScanners) {
            if (checkForScanner(valueList, scn.getScannerIndex())) continue;
          }
          boolean hasMore = scn.next(intermediateResult);
          if (!hasRangeScanners) {
            if (intermediateResult != null && !intermediateResult.isEmpty()) {
              byte[] rowKey = IndexUtils.getRowKeyFromKV(intermediateResult.get(0));
              if (maxRowKey == null) {
                maxRowKey = rowKey;
              } else {
                int result = Bytes.compareTo(maxRowKey, rowKey);
                if (haveSameRows) haveSameRows = (result == 0);
                maxRowKey = result > 0 ? maxRowKey : rowKey;
              }
              if (kv == null) kv = intermediateResult.get(0);
              intermediateResult.clear();
              valueList.add(new Pair<byte[], IndexRegionScanner>(rowKey, scn));
            }
          } else {
            if (!intermediateResult.isEmpty()) {
              boolean matching =
                  checkAndPutMatchingEntry(intermediateResult.get(0), scn.getScannerIndex());
              if (matching) {
                results.addAll(intermediateResult);
              }
              intermediateResult.clear();
            }
          }
          if (!hasMore) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removing scanner " + scn + " from the list.");
            }
            scn.close();
            scnItr.remove();
            if (hasRangeScanners) {
              // TODO: we can remove unnecessary rows(which never become matching entries) from
              // cache on scanner close.
              if (this.scanners.isEmpty()) return false;
            }
            if (results.size() > 0) {
              break;
            }
          }
          if (results.size() > 0) {
            return !this.scanners.isEmpty();
          }
        }
        if (!hasRangeScanners) {
          if (haveSameRows && valueList.size() == scannersCount) {
            if (kv != null) results.add(kv);
            return this.scanners.size() == scannersCount;
          } else if (haveSameRows && valueList.size() != scannersCount) {
            close();
            return false;
          } else {
            // In case of AND if the reseek on any one scanner returns false
            // we can close the entire scanners in the AND subtree
            if (!reseekTheScanners(valueList, maxRowKey)) {
              close();
              return false;
            }
          }
        }
      }
      if (hasRangeScanners) {
        return true;
      } else {
        return this.scanners.size() == scannersCount;
      }
    }
    return false;
  }

  private boolean checkForScanner(List<Pair<byte[], IndexRegionScanner>> valueList, int scnIndex) {
    for (Pair<byte[], IndexRegionScanner> value : valueList) {
      if (value.getSecond().getScannerIndex() == scnIndex) {
        return true;
      }
    }
    return false;
  }

  private boolean checkAndPutMatchingEntry(Cell cell, int index) {
    String rowKeyFromKV = Bytes.toString(IndexUtils.getRowKeyFromKV(cell));
    Pair<List<Boolean>, Integer> countPair = rowCache.get(rowKeyFromKV);
    if (countPair == null) {
      // If present scanners count is not equal to actual scanners count,no need to put row key into
      // cache because this will never become matching entry.
      if (this.scanners.size() == scannersCount) {
        List<Boolean> scannerFlags = new ArrayList<Boolean>(scannerIndex);
        for (int i = 0; i < scannersCount; i++) {
          scannerFlags.add(false);
        }
        countPair = new Pair<List<Boolean>, Integer>(scannerFlags, 0);
        // TODO verify
        // rowCache.put(rowKeyFromKV, countPair);
      } else {
        return false;
      }
    }
    Boolean indexFlag = countPair.getFirst().get(index);
    Integer countObj = countPair.getSecond();
    // If count is equal to scanner count before increment means its returned already. skip the
    // result.
    if (countObj == scannersCount) {
      return false;
    }
    if (!indexFlag) {
      countObj++;
      countPair.getFirst().set(index, true);
      countPair.setSecond(countObj);
      rowCache.put(rowKeyFromKV, countPair);
    }
    if (countObj == scannersCount) {
      return true;
    } else {
      // If the difference between actual scanner count(scannerCount) and number of scanners have
      // this row(countObj) is more than present scanners size then remove row from cache because
      // this never be maching entry.
      if ((scannersCount - countObj) > this.scanners.size()) {
        rowCache.remove(rowKeyFromKV);
        return false;
      }
    }
    return false;
  }

  private boolean reseekTheScanners(List<Pair<byte[], IndexRegionScanner>> valueList,
      byte[] maxRowKey) throws IOException {
    Iterator<Pair<byte[], IndexRegionScanner>> itr = valueList.iterator();
    while (itr.hasNext()) {
      Pair<byte[], IndexRegionScanner> rowVsScanner = itr.next();
      IndexRegionScanner scn = rowVsScanner.getSecond();
      // We need to call reseek on OR scanner even the last returned row key is equal or more than
      // max row key to set reseek flag.
      if (scn instanceof IndexRegionScannerForOR) {
        rowVsScanner.getSecond().reseek(maxRowKey);
        itr.remove();
        continue;
      }
      if (Bytes.compareTo(rowVsScanner.getFirst(), maxRowKey) < 0) {
        if (!scn.reseek(maxRowKey)) {
          return false;
        }
        itr.remove();
      }
    }
    return true;
  }

  @Override
  public boolean next(List<Cell> result, int limit) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long getMaxResultSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getMvccReadPoint() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean nextRaw(List<Cell> result) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nextRaw(List<Cell> result, int limit) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

}
