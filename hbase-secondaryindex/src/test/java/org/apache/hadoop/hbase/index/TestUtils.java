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
package org.apache.hadoop.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestUtils {

  public static HTableDescriptor createIndexedHTableDescriptor(String tableName,
      String columnFamily, String indexName, String indexColumnFamily, String indexColumnQualifier)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    IndexSpecification iSpec = new IndexSpecification(indexName);
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    TableIndices tableIndices = new TableIndices();
    tableIndices.addIndex(iSpec);
    iSpec.addIndexColumn(hcd, indexColumnQualifier, ValueType.String, 10);
    htd.addFamily(hcd);
    htd.setValue(Constants.INDEX_SPEC_KEY, tableIndices.toByteArray());
    return htd;
  }

  public static void waitUntilIndexTableCreated(HMaster master, String tableName)
      throws IOException, InterruptedException {
    boolean isEnabled = false;
    boolean isExist = false;
    do {
      isExist = MetaReader.tableExists(master.getCatalogTracker(), TableName.valueOf(tableName));
      isEnabled =
          master.getAssignmentManager().getZKTable().isEnabledTable(TableName.valueOf(tableName));
      Thread.sleep(1000);
    } while ((false == isExist) && (false == isEnabled));
  }

  public static List<Pair<byte[], ServerName>> getStartKeysAndLocations(HMaster master,
      String tableName) throws IOException, InterruptedException {

    List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations =
        MetaReader.getTableRegionsAndLocations(master.getCatalogTracker(),
          TableName.valueOf(tableName));
    List<Pair<byte[], ServerName>> startKeyAndLocationPairs =
        new ArrayList<Pair<byte[], ServerName>>(tableRegionsAndLocations.size());
    Pair<byte[], ServerName> startKeyAndLocation = null;
    for (Pair<HRegionInfo, ServerName> regionAndLocation : tableRegionsAndLocations) {
      startKeyAndLocation =
          new Pair<byte[], ServerName>(regionAndLocation.getFirst().getStartKey(),
              regionAndLocation.getSecond());
      startKeyAndLocationPairs.add(startKeyAndLocation);
    }
    return startKeyAndLocationPairs;

  }

  public static boolean checkForColocation(HMaster master, String tableName, String indexTableName)
      throws IOException, InterruptedException {
    List<Pair<byte[], ServerName>> uTableStartKeysAndLocations =
        getStartKeysAndLocations(master, tableName);
    List<Pair<byte[], ServerName>> iTableStartKeysAndLocations =
        getStartKeysAndLocations(master, indexTableName);

    boolean regionsColocated = true;
    if (uTableStartKeysAndLocations.size() != iTableStartKeysAndLocations.size()) {
      regionsColocated = false;
    } else {
      for (int i = 0; i < uTableStartKeysAndLocations.size(); i++) {
        Pair<byte[], ServerName> uStartKeyAndLocation = uTableStartKeysAndLocations.get(i);
        Pair<byte[], ServerName> iStartKeyAndLocation = iTableStartKeysAndLocations.get(i);

        if (Bytes.compareTo(uStartKeyAndLocation.getFirst(), iStartKeyAndLocation.getFirst()) == 0) {
          if (uStartKeyAndLocation.getSecond().equals(iStartKeyAndLocation.getSecond())) {
            continue;
          }
        }
        regionsColocated = false;
      }
    }
    return regionsColocated;
  }

}
