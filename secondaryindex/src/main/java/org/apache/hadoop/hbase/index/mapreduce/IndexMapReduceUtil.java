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
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexMapReduceUtil {

  public static final String INDEX_DATA_DIR = ".index";

  public static final String INDEX_IS_INDEXED_TABLE = "indeximporttsv.isindexedtable";

  static Log LOG = LogFactory.getLog(IndexMapReduceUtil.class);

  private static IndexedHTableDescriptor hTableDescriptor = null;
  private static String tblName = null;

  private static byte[][] startKeys = null;

  public static IndexedHTableDescriptor getTableDescriptor(String tableName, Configuration conf)
      throws IOException {
    if (hTableDescriptor == null || !tableName.equals(hTableDescriptor.getNameAsString())) {
      hTableDescriptor = IndexUtils.getIndexedHTableDescriptor(Bytes.toBytes(tableName), conf);
    }
    return hTableDescriptor;
  }

  public static boolean isIndexedTable(String tableName, Configuration conf) throws IOException {
    IndexedHTableDescriptor tableDescriptor = getTableDescriptor(tableName, conf);
    return tableDescriptor != null;
  }

  public static boolean isIndexedTable(Configuration conf) throws IOException {
    String tableName = conf.get(TableInputFormat.INPUT_TABLE);
    return isIndexedTable(tableName, conf);
  }

  public static List<Put> getIndexPut(Put userPut, Configuration conf) throws IOException {
    String tableName = conf.get(TableInputFormat.INPUT_TABLE);
    IndexedHTableDescriptor tableDescriptor = getTableDescriptor(tableName, conf);
    List<Put> indexPuts = new ArrayList<Put>();
    if (tableDescriptor != null) {
      List<IndexSpecification> indices = tableDescriptor.getIndices();
      for (IndexSpecification index : indices) {
        byte[] startkey = getStartKey(conf, tableName, userPut.getRow());
        Put indexPut = IndexUtils.prepareIndexPut(userPut, index, startkey);
        if (indexPut != null) {
          indexPuts.add(indexPut);
        }
      }
    }
    return indexPuts;
  }

  public static List<Delete> getIndexDelete(Delete userDelete, Configuration conf)
      throws IOException {
    String tableName = conf.get(TableInputFormat.INPUT_TABLE);
    IndexedHTableDescriptor tableDescriptor = getTableDescriptor(tableName, conf);
    List<Delete> indexDeletes = new ArrayList<Delete>();
    if (tableDescriptor != null) {
      List<IndexSpecification> indices = tableDescriptor.getIndices();
      for (IndexSpecification index : indices) {
        byte[] startkey = getStartKey(conf, tableName, userDelete.getRow());
        Delete indexDelete = IndexUtils.prepareIndexDelete(userDelete, index, startkey);
        if (indexDelete != null) {
          indexDeletes.add(indexDelete);
        }
      }
    }
    return indexDeletes;
  }

  public static byte[] getStartKey(Configuration conf, String tableName, byte[] row)
      throws IOException {

    if (startKeys == null || startKeys.length == 0 || !tableName.equals(tblName)) {
      tblName = tableName;
      HTable table = null;
      try {
        table = new HTable(conf, tableName);
        startKeys = table.getStartKeys();
      } finally {
        if (table != null) {
          table.close();
        }
      }
    }
    if (startKeys.length != 0) {
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
