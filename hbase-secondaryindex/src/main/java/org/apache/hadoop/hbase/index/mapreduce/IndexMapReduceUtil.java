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
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexMapReduceUtil {

  public static final String INDEX_DATA_DIR = ".index";

  public static final String IS_INDEXED_TABLE = "indeximporttsv.isindexedtable";

  static Log LOG = LogFactory.getLog(IndexMapReduceUtil.class);

  public static HTableDescriptor getTableDescriptor(String tableName, Configuration conf)
      throws IOException {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(conf);
      return admin.getTableDescriptor(TableName.valueOf(tableName));
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static boolean isIndexedTable(String tableName, Configuration conf) throws IOException {
    return getTableDescriptor(tableName, conf).getValue(Constants.INDEX_SPEC_KEY) != null;
  }

  public static List<Put> getIndexPut(Put userPut, List<IndexSpecification> indices,
      byte[][] startKeys, Configuration conf) throws IOException {
    List<Put> indexPuts = new ArrayList<Put>();
    for (IndexSpecification index : indices) {
      byte[] startkey = getStartKey(conf, startKeys, userPut.getRow());
      Put indexPut = IndexUtils.prepareIndexPut(userPut, index, startkey);
      if (indexPut != null) {
        indexPuts.add(indexPut);
      }
    }
    return indexPuts;
  }

  public static byte[] getStartKey(Configuration conf, byte[][] startKeys, byte[] row)
      throws IOException {
    if (startKeys != null && startKeys.length != 0) {
      for (int i = 0; i < (startKeys.length - 1); i++) {
        int diff = Bytes.compareTo(row, startKeys[i]);
        if (diff == 0 || (diff > 0 && Bytes.compareTo(row, startKeys[i + 1]) < 0)) {
          return startKeys[i];
        }
      }
      return startKeys[startKeys.length - 1];
    }
    return null;
  }

}
