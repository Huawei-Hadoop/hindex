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
package org.apache.hadoop.hbase.index.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HConnectable;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsckRepair;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.Table.State;

import org.apache.zookeeper.KeeperException;

public class SecondaryIndexColocator {

  public static final Log LOG = LogFactory.getLog(SecondaryIndexColocator.class);

  public Configuration conf = null;

  private Map<String, List<MetaInfo>> tableMap = new HashMap<String, List<MetaInfo>>();

  private List<HRegionInfo> regionsToMove = new ArrayList<HRegionInfo>();

  private Map<ServerName, Set<HRegionInfo>> rsToRegionMap =
      new HashMap<ServerName, Set<HRegionInfo>>();

  private Map<byte[], State> disabledandDisablingTables = new TreeMap<byte[], State>(
      Bytes.BYTES_COMPARATOR);

  private Map<byte[], State> enabledOrEnablingTables = new TreeMap<byte[], State>(
      Bytes.BYTES_COMPARATOR);

  // private Set<HRegionInfo> staleMetaEntries = new HashSet<HRegionInfo>();

  private List<Pair<String, State>> tablesToBeSetInZK = new ArrayList<Pair<String, State>>();

  // List<String> rit = new ArrayList<String>();

  private IndexAdmin admin;
  private ClusterStatus status;
  private HConnection connection;
  private ZooKeeperWatcher watcher;

  public SecondaryIndexColocator(Configuration conf) {
    this.conf = conf;
  }

  public void setUp() throws IOException, ZooKeeperConnectionException {
    admin = new IndexAdmin(conf);
    status = admin.getClusterStatus();
    connection = admin.getConnection();
    watcher = createZooKeeperWatcher();
  }

  public static void main(String args[]) throws Exception {
    Configuration config = HBaseConfiguration.create();
    SecondaryIndexColocator secHbck = null;
    try {
      secHbck = new SecondaryIndexColocator(config);
      secHbck.setUp();
      secHbck.admin.setBalancerRunning(false, true);
      boolean inconsistent = secHbck.checkForCoLocationInconsistency();
      if (inconsistent) {
        secHbck.fixCoLocationInconsistency();
      }
      secHbck.admin.setBalancerRunning(true, true);
    } finally {
      if (secHbck != null) {
        if (secHbck.admin != null) secHbck.admin.close();
        if (secHbck.watcher != null) secHbck.watcher.close();
      }
    }
  }

  public boolean checkForCoLocationInconsistency() throws IOException, KeeperException,
      InterruptedException {
    getMetaInfo();
    // Do we need to complete the partial disable table
    loadDisabledTables();
    // handleRIT();
    checkMetaInfoCosistency();
    setTablesInZK();
    // in the former steps there may have been movement of regions. So need to update
    // in memory map of tables.
    getMetaInfo();
    checkCoLocationAndGetRegionsToBeMoved();
    this.watcher.close();
    if (regionsToMove == null || regionsToMove.isEmpty()) {
      return false;
    }
    return true;
  }

  /*
   * private void handleRIT() { if(rit != null && !rit.isEmpty()){ for(String s : rit){
   * RegionTransitionData data = ZKAssign.getDataNoWatch(zkw, pathOrRegionName, stat) } } }
   */

  private void checkMetaInfoCosistency() throws IOException, KeeperException, InterruptedException {
    if (status == null) {
      throw new IOException("Cluster status is not available.");
    }
    Collection<ServerName> regionServers = status.getServers();
    for (ServerName serverName : regionServers) {
      BlockingInterface server = connection.getAdmin(serverName);
      Set<HRegionInfo> onlineRegions = new HashSet<HRegionInfo>();
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(server);
      if (regions == null) continue;
      onlineRegions.addAll(regions);
      if (rsToRegionMap == null) {
        rsToRegionMap = new HashMap<ServerName, Set<HRegionInfo>>();
      }
      rsToRegionMap.put(serverName, onlineRegions);
    }
    if (tableMap != null && !tableMap.isEmpty()) {
      for (Map.Entry<String, List<MetaInfo>> e : tableMap.entrySet()) {
        if (isDisabledOrDisablingTable(Bytes.toBytes(e.getKey()))) {
          // It should be disabled...But we can check this
          if (disabledandDisablingTables.get(Bytes.toBytes(e.getKey())) == State.DISABLED) {
            continue;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table " + e.getKey()
                  + " is in DISABLING state. Trying to close all the regions for this table.");
            }
            // If the table is in DISABLING state , then there might be some regions which are
            // still online. Close the regions and set the table as DISABLED.
            for (MetaInfo metaInfo : e.getValue()) {
              List<ServerName> sn = new ArrayList<ServerName>();
              for (Map.Entry<ServerName, Set<HRegionInfo>> entry : rsToRegionMap.entrySet()) {
                if (entry.getValue().contains(metaInfo.getRegionInfo())) {
                  sn.add(entry.getKey());
                }
              }
              if (sn.isEmpty()) {
                // region is not assigned anywhere ,so continue.
                continue;
              } else {
                HBaseFsckRepair.fixMultiAssignment(this.admin, metaInfo.getRegionInfo(), sn);
              }
            }
            Pair<String, State> p = new Pair<String, State>();
            p.setFirst(e.getKey());
            p.setSecond(State.DISABLED);
            tablesToBeSetInZK.add(p);
          }
        } else {
          // first we are checking here for the tables to be enabled which
          // we left in disabled stage in the previous step.
          if (!disabledandDisablingTables.containsKey(Bytes.toBytes(e.getKey()))
              && !enabledOrEnablingTables.containsKey(Bytes.toBytes(e.getKey()))) {
            // if reached here then this table, which is disabled
            // and still not present in our in-memory map of disabledTables ,
            // should
            // be enabled.
            this.admin.enableTable(e.getKey());
            this.enabledOrEnablingTables.put(Bytes.toBytes(e.getKey()), State.ENABLED);
            continue;
          }
          boolean movedRegions = false;
          for (MetaInfo metaInfo : e.getValue()) {
            List<ServerName> sn = new ArrayList<ServerName>();
            for (Map.Entry<ServerName, Set<HRegionInfo>> entry : rsToRegionMap.entrySet()) {
              if (entry.getValue().contains(metaInfo.getRegionInfo())) {
                sn.add(entry.getKey());
              }
            }
            if (sn.size() == 1 && sn.get(0).equals(metaInfo.getServerName())) {
              // this means region is deployed on correct rs according to META.
              if (LOG.isDebugEnabled()) {
                LOG.debug("Info in META for region "
                    + metaInfo.getRegionInfo().getRegionNameAsString() + " is correct.");
              }
              continue;
            }
            // if it reaches here , it means that the region is deployed and
            // in some other rs. Need to find it and call unassign.
            if (sn.isEmpty()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Region " + metaInfo.getRegionInfo().getRegionNameAsString()
                    + " not deployed on any rs.Trying to assign");
              }
              HBaseFsckRepair.fixUnassigned(this.admin, metaInfo.getRegionInfo());
              HBaseFsckRepair.waitUntilAssigned(this.admin, metaInfo.getRegionInfo());
              movedRegions = true;
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Region " + metaInfo.getRegionInfo().getRegionNameAsString()
                    + " is not deployed on the rs as mentioned in META. Re-assigning.");
              }
              HBaseFsckRepair.fixMultiAssignment(this.admin, metaInfo.getRegionInfo(), sn);
              HBaseFsckRepair.waitUntilAssigned(this.admin, metaInfo.getRegionInfo());
              movedRegions = true;
            }
          }
          if (movedRegions) {
            Pair<String, State> p = new Pair<String, State>();
            p.setFirst(e.getKey());
            p.setSecond(State.ENABLED);
            tablesToBeSetInZK.add(p);
          }
        }
      }
    }
  }

  private void setTablesInZK() throws IOException, KeeperException {
    if (tablesToBeSetInZK != null && !tablesToBeSetInZK.isEmpty()) {
      ZKTable zkTable = new ZKTable(this.watcher);
      for (Pair<String, State> p : tablesToBeSetInZK) {
        setStateInZK(zkTable, p.getFirst(), p.getSecond());
      }
    }
  }

  private void setStateInZK(ZKTable zkTable, String tableName, State state) throws IOException,
      KeeperException {
    if (state == State.ENABLED) {
      zkTable.setEnabledTable(TableName.valueOf(tableName));
    }
    if (state == State.DISABLED) {
      zkTable.setDisabledTable(TableName.valueOf(tableName));
    }
  }

  private boolean isDisabledOrDisablingTable(byte[] tableName) {
    if (disabledandDisablingTables != null && !disabledandDisablingTables.isEmpty()) {
      if (disabledandDisablingTables.containsKey(tableName)) return true;
    }
    return false;
  }

  public void fixCoLocationInconsistency() throws HBaseIOException {
    if (regionsToMove != null && !regionsToMove.isEmpty()) {
      Iterator<HRegionInfo> itr = regionsToMove.iterator();
      while (itr.hasNext()) {
        HRegionInfo hri = itr.next();
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Moving region " + hri.getRegionNameAsString() + " to server ");
          }
          admin.move(hri.getEncodedNameAsBytes(), null);
          itr.remove();
        } catch (UnknownRegionException e) {
          LOG.error("Unnkown region exception.", e);
        } catch (MasterNotRunningException e) {
          LOG.error("Master not running.", e);
        } catch (ZooKeeperConnectionException e) {
          LOG.error("Zookeeper connection exception.", e);
        }
      }
    }
  }

  private void checkCoLocationAndGetRegionsToBeMoved() {
    if (tableMap != null && !tableMap.isEmpty()) {
      Iterator<Map.Entry<String, List<MetaInfo>>> itr = tableMap.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry<String, List<MetaInfo>> e = itr.next();
        if (!IndexUtils.isIndexTable(e.getKey())
            && !IndexUtils.isCatalogOrSystemTable(TableName.valueOf(e.getKey()))) {
          if (isDisabledOrDisablingTable(Bytes.toBytes(e.getKey()))) continue;
          String indexTableName = IndexUtils.getIndexTableName(e.getKey());
          List<MetaInfo> idxRegionList = tableMap.get(indexTableName);
          if (idxRegionList == null || idxRegionList.isEmpty()) {
            itr.remove();
            continue;
          } else {
            getRegionsToMove(e.getValue(), idxRegionList);
            itr.remove();
          }
        }
      }
    }
  }

  private void getRegionsToMove(List<MetaInfo> userRegions, List<MetaInfo> idxRegionList) {
    Iterator<MetaInfo> userRegionItr = userRegions.iterator();
    while (userRegionItr.hasNext()) {
      MetaInfo userRegionMetaInfo = userRegionItr.next();
      for (MetaInfo indexRegionMetaInfo : idxRegionList) {
        if (Bytes.equals(userRegionMetaInfo.getRegionInfo().getStartKey(), indexRegionMetaInfo
            .getRegionInfo().getStartKey())) {
          if (!userRegionMetaInfo.getServerName().equals(indexRegionMetaInfo.getServerName())) {
            if (regionsToMove == null) {
              regionsToMove = new ArrayList<HRegionInfo>();
            }
            regionsToMove.add(userRegionMetaInfo.getRegionInfo());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Adding region "
                  + userRegionMetaInfo.getRegionInfo().getRegionNameAsString()
                  + " to regions to be moved list.");
            }
          }
          break;
        }
      }
    }
  }

  private void getMetaInfo() throws IOException {

    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {

      // comparator to sort KeyValues with latest timestamp
      final Comparator<Cell> comp = new Comparator<Cell>() {
        public int compare(Cell k1, Cell k2) {
          return (int) (k1.getTimestamp() - k2.getTimestamp());
        }
      };

      public boolean processRow(Result result) throws IOException {
        try {

          long ts = Collections.max(result.list(), comp).getTimestamp();
          // record the latest modification of this META record
          HRegionInfo info = HRegionInfo.getHRegionInfo(result);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + result);
            return true;
          }

          Pair<HRegionInfo, ServerName> pair =
              new Pair<HRegionInfo, ServerName>(info, HRegionInfo.getServerName(result));
          if (pair != null) {
            String tableName = pair.getFirst().getTable().getNameAsString();
            if (tableMap == null) {
              tableMap = new HashMap<String, List<MetaInfo>>();
            }
            List<MetaInfo> regionsOfTable = tableMap.get(tableName);
            if (regionsOfTable == null) {
              regionsOfTable = new ArrayList<MetaInfo>();
              tableMap.put(tableName, regionsOfTable);
            }
            Iterator<MetaInfo> itr = regionsOfTable.iterator();
            while (itr.hasNext()) {
              MetaInfo m = itr.next();
              if (m.getRegionInfo().equals(pair.getFirst())) {
                itr.remove();
                break;
              }
            }
            MetaInfo m = new MetaInfo(pair.getFirst(), pair.getSecond(), ts);
            regionsOfTable.add(m);
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };
    // Scan -ROOT- to pick up META regions
    MetaScanner.metaScan(conf, visitor, null, null, Integer.MAX_VALUE);

    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);

  }

  /**
   * Stores the regioninfo entries scanned from META
   */
  static class MetaInfo {
    private HRegionInfo hri;
    private ServerName regionServer;
    private long timeStamp;

    public MetaInfo(HRegionInfo hri, ServerName regionServer, long modTime) {
      this.hri = hri;
      this.regionServer = regionServer;
      this.timeStamp = modTime;
    }

    public HRegionInfo getRegionInfo() {
      return this.hri;
    }

    public ServerName getServerName() {
      return this.regionServer;
    }

    public long getTimeStamp() {
      return this.timeStamp;
    }
  }

  public void setAdmin(IndexAdmin admin, HConnection conn, ClusterStatus status) {
    this.admin = admin;
    this.connection = conn;
    this.status = status;
  }

  private void loadDisabledTables() throws ZooKeeperConnectionException, IOException,
      KeeperException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        try {
          for (Entry<State, Set<TableName>> e : ZKTableReadOnly.getTableNamesByState(watcher)
              .entrySet()) {
            if (e.getKey() == ZooKeeperProtos.Table.State.DISABLED
                || e.getKey() == ZooKeeperProtos.Table.State.DISABLING) {
              for (TableName tableName : e.getValue()) {
                disabledandDisablingTables.put(tableName.getName(), e.getKey());
              }
            } else if (e.getKey() == ZooKeeperProtos.Table.State.ENABLED
                || e.getKey() == ZooKeeperProtos.Table.State.ENABLING) {
              for (TableName tableName : e.getValue()) {
                enabledOrEnablingTables.put(tableName.getName(), e.getKey());
              }
            }
          }
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
        return null;
      }
    });
    checkDisabledAndEnabledTables();
  }

  private void checkDisabledAndEnabledTables() throws IOException, KeeperException {
    if (disabledandDisablingTables != null && !disabledandDisablingTables.isEmpty()) {
      Map<byte[], State> disabledHere = new TreeMap<byte[], State>(Bytes.BYTES_COMPARATOR);
      Iterator<Entry<byte[], State>> itr = disabledandDisablingTables.entrySet().iterator();
      while (itr.hasNext()) {
        Entry<byte[], State> tableEntry = itr.next();
        if (!IndexUtils.isIndexTable(tableEntry.getKey())) {
          byte[] indexTableName = Bytes.toBytes(IndexUtils.getIndexTableName(tableEntry.getKey()));
          if (null == tableMap.get(Bytes.toString(indexTableName))) {
            continue;
          }
          boolean present = disabledandDisablingTables.containsKey(indexTableName);
          if (!present && (enabledOrEnablingTables.get(indexTableName) == State.ENABLED)) {
            // TODO How to handle ENABLING state(if it could happen). If try to disable ENABLING
            // table
            // it throws.
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table " + Bytes.toString(tableEntry.getKey())
                  + " is disabled but corresponding index table is " + "enabled. So disabling "
                  + Bytes.toString(indexTableName));
            }
            this.admin.disableTable(indexTableName);
            disabledHere.put(indexTableName, State.DISABLED);
          }
        } else {
          if (tableEntry.getValue() != State.DISABLED) {
            continue;
          }
          byte[] userTableName =
              Bytes.toBytes(IndexUtils.getActualTableName(Bytes.toString(tableEntry.getKey())));
          if (!disabledandDisablingTables.containsKey(userTableName)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Index Table " + Bytes.toString(tableEntry.getKey())
                  + " is disabled but corresponding user table is enabled. So Enabling "
                  + Bytes.toString(tableEntry.getKey()));
            }
            // Here we are not enabling the table. We will do it in the next step
            // checkMetaInfoCosistency().
            // Because if we do here, META will be updated and our in-memory map will have old
            // entries.
            // So it will surely cause unnecessary unassignments and assignments in the next step.
            // In the next
            // step anyway we are moving regions. So no problem doing it there.
            // this.admin.enableTable(tableName);
            itr.remove();
          }
        }
      }
      disabledandDisablingTables.putAll(disabledHere);
    }
  }

  private ZooKeeperWatcher createZooKeeperWatcher() throws IOException {
    return new ZooKeeperWatcher(this.conf, "sec index colocation", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.error(why, e);
        System.exit(1);
      }

      @Override
      public boolean isAborted() {
        return false;
      }

    });
  }

}