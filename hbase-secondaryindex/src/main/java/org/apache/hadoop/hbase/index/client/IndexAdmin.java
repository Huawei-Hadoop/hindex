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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extension of HBaseAdmin to perform index related operations. This also should be used when we
 * create table with index details, and other admin operations on indexed table.
 */
public class IndexAdmin extends HBaseAdmin {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  public IndexAdmin(Configuration c) throws IOException {
    super(c);
  }

  public IndexAdmin(HConnection connection) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    super(connection);
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

}
