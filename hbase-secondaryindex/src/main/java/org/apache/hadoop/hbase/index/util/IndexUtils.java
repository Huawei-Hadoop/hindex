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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

public class IndexUtils {

  private static final Log LOG = LogFactory.getLog(IndexUtils.class);

  public static final String TABLE_INPUT_COLS = "table.columns.index";

  /**
   * Utility method to get the name of the index table when given the name of the actual table.
   * @param tableName
   * @return index table name
   */
  public static String getIndexTableName(String tableName) {
    // TODO The suffix for the index table is fixed now. Do we allow to make this configurable?
    // We can handle things in byte[] way?
    return tableName + Constants.INDEX_TABLE_SUFFIX;
  }

  /**
   * @param tableName
   * @return index table name
   */
  public static String getIndexTableName(TableName tableName) {
    return getIndexTableName(tableName.getNameAsString());
  }

  /**
   * Utility method to get the name of the index table when given the name of the actual table.
   * @param tableName
   * @return index table name
   */
  public static String getIndexTableName(byte[] tableName) {
    return getIndexTableName(Bytes.toString(tableName));
  }

  /**
   * Tells whether the passed table is a secondary index table or a normal table.
   * @param tableName
   * @return true if the table is index table otherwise false.
   */
  public static boolean isIndexTable(String tableName) {
    return tableName.endsWith(Constants.INDEX_TABLE_SUFFIX);
  }

  public static boolean isIndexTable(TableName tableName) {
    return tableName.getQualifierAsString().endsWith(Constants.INDEX_TABLE_SUFFIX);
  }

  public static byte[] isLegalIndexName(final byte[] indexName) {
    if (indexName == null || indexName.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    if (indexName[0] == '.' || indexName[0] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + indexName[0]
          + "> at 0. Index names can only start with 'word " + "characters': i.e. [a-zA-Z_0-9]: "
          + Bytes.toString(indexName));
    }
    for (int i = 0; i < indexName.length; i++) {
      if (Character.isLetterOrDigit(indexName[i]) || indexName[i] == '_' || indexName[i] == '-'
          || indexName[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + indexName[i] + "> at " + i
          + ". Index names can only contain " + "'word characters': i.e. [a-zA-Z_0-9-.]: "
          + Bytes.toString(indexName));
    }
    return indexName;
  }

  /**
   * Tells whether the passed table is a secondary index table or a normal table.
   * @param tableName
   * @return true if the table is index table otherwise false.
   */
  public static boolean isIndexTable(byte[] tableName) {
    return isIndexTable(Bytes.toString(tableName));
  }

  /**
   * Checks whether the passed table is a catalog table or not
   * @param tableName
   * @return true when the passed table is a catalog table.
   */
  public static boolean isCatalogOrSystemTable(TableName tableName) {
    return tableName.equals(TableName.META_TABLE_NAME)
        || tableName.equals(TableName.NAMESPACE_TABLE_NAME) || tableName.isSystemTable();
  }

  /**
   * Returns the max length allowed for the index name.
   * @return default max index name length.
   */
  public static int getMaxIndexNameLength() {
    // TODO we need to allow customers to configure this value.
    return Constants.DEF_MAX_INDEX_NAME_LENGTH;
  }

  /**
   * Returns the main table name.
   * @param indexTableName
   * @return actual table name.
   */
  public static String extractActualTableName(String indexTableName) {
    int endIndex = indexTableName.length() - Constants.INDEX_TABLE_SUFFIX.length();
    return indexTableName.substring(0, endIndex);
  }

  public static byte[] changeValueAccToDataType(byte[] value, ValueType valueType) {
    byte[] valueArr = new byte[value.length];
    System.arraycopy(value, 0, valueArr, 0, value.length);

    if (valueArr.length == 0) return valueArr;
    switch (valueType) {
    case String:
    case Char:
      break;
    case Float:
      float f = Bytes.toFloat(valueArr);
      if (f > 0) {
        valueArr[0] ^= (1 << 7);
      } else {
        valueArr[0] ^= 0xff;
        valueArr[1] ^= 0xff;
        valueArr[2] ^= 0xff;
        valueArr[3] ^= 0xff;
      }
      break;
    case Double:
      double d = Bytes.toDouble(valueArr);
      if (d > 0) {
        valueArr[0] ^= (1 << 7);
      } else {
        for (int i = 0; i < 8; i++) {
          valueArr[i] ^= 0xff;
        }
      }
      break;
    case Int:
    case Long:
    case Short:
    case Byte:
      valueArr[0] ^= (1 << 7);
      break;
    }
    return valueArr;
  }

  // TODO check this... Is this ok with all cases?
  // No.. for -ve issues... Will see later..
  public static byte[] incrementValue(byte[] value, boolean copy) {
    byte[] newValue = new byte[value.length];
    if (copy) {
      System.arraycopy(value, 0, newValue, 0, newValue.length);
    } else {
      newValue = value;
    }
    for (int i = newValue.length - 1; i >= 0; i--) {
      byte b = newValue[i];
      b = (byte) (b + 1);
      if (b == 0) {
        newValue[i] = 0;
      } else {
        newValue[i] = b;
        break;
      }
    }
    return newValue;
  }

  public static String getActualTableName(String indexTableName) {
    String split[] = indexTableName.split(Constants.INDEX_TABLE_SUFFIX);
    return split[0];
  }

  public static TableName getActualTableName(TableName indexTable) {
    return TableName.valueOf(getActualTableName(indexTable.getNameAsString()));
  }

  public static Put prepareIndexPut(Put userPut, IndexSpecification index, HRegion indexRegion)
      throws IOException {
    byte[] indexRegionStartKey = indexRegion.getStartKey();
    return prepareIndexPut(userPut, index, indexRegionStartKey);
  }

  public static Delete prepareIndexDelete(Delete userDelete, IndexSpecification index,
      byte[] indexRegionStartKey) throws IOException {
    ByteArrayBuilder indexRow =
        IndexUtils.getIndexRowKeyHeader(index, indexRegionStartKey, userDelete.getRow());
    boolean update = false;
    for (ColumnQualifier cq : index.getIndexColumns()) {
      Cell kvFound = null;
      for (Entry<byte[], List<Cell>> entry : userDelete.getFamilyCellMap().entrySet()) {
        for (Cell cell : entry.getValue()) {
          Cell kv = KeyValueUtil.ensureKeyValue(cell);
          if (Bytes.equals(cq.getColumnFamily(), kv.getFamily())
              && Bytes.equals(cq.getQualifier(), kv.getQualifier())) {
            kvFound = kv;
            update = true;
            break;
          }
        }
      }
      if (kvFound == null) {
        indexRow.position(indexRow.position() + cq.getMaxValueLength());
      } else {
        IndexUtils.updateRowKeyForKV(cq, kvFound, indexRow);
      }
    }
    if (update) {
      // Append the actual row key at the end of the index row key.
      indexRow.put(userDelete.getRow());
      Delete idxDelete = new Delete(indexRow.array());
      idxDelete.deleteColumn(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL,
        userDelete.getTimeStamp());
      idxDelete.setDurability(Durability.SKIP_WAL);
      return idxDelete;
    }
    return null;
  }

  // Default access specifier for the UT
  public static Put prepareIndexPut(Put userPut, IndexSpecification index,
      byte[] indexRegionStartKey) throws IOException {
    long tsForIndexTabPut = 0;

    boolean bypass = true;
    for (ColumnQualifier c : index.getIndexColumns()) {
      List<Cell> values = userPut.get(c.getColumnFamily(), c.getQualifier());
      if (null != values && values.size() > 0) {
        bypass = false;
        break;
      }
    }
    if (bypass) {
      // When this Put having no values for all the column in this index just skip this Put
      // from adding corresponding entry in the index table.
      return null;
    }
    byte[] primaryRowKey = userPut.getRow();
    ByteArrayBuilder indexRowKey = getIndexRowKeyHeader(index, indexRegionStartKey, primaryRowKey);

    // STEP 3 : Adding the column value + padding for each of the columns in
    // the index.
    for (ColumnQualifier indexCQ : index.getIndexColumns()) {
      List<Cell> values = userPut.get(indexCQ.getColumnFamily(), indexCQ.getQualifier());
      if (values == null || values.isEmpty()) {
        // There is no value provided for the column. Going with the padding
        // All the bytes in the byte[] 'indexRowKey' will be 0s already.
        // No need to put a 0 padding bytes. Just need to advance the position by col max value
        // length.
        indexRowKey.position(indexRowKey.position() + indexCQ.getMaxValueLength());
      } else {
        // A put can contains diff version values for the same column.
        // We can consider the latest value only for the indexing. This needs to be documented.
        // TODO
        Cell kv = selectKVForIndexing(values);
        updateRowKeyForKV(indexCQ, kv, indexRowKey);
        if (tsForIndexTabPut < kv.getTimestamp()) {
          tsForIndexTabPut = kv.getTimestamp();
        }
      }
    }
    // Remember the offset of rowkey and store it as value
    short rowKeyOffset = indexRowKey.position();

    // STEP 4 : Adding the user table rowkey.
    indexRowKey.put(primaryRowKey);

    // Creating the value to be put into the index column
    // Last portion of index row key = [region start key length (2 bytes), offset of primary rowkey
    // in index rowkey (2 bytes)]
    ByteArrayBuilder indexColVal = ByteArrayBuilder.allocate(4);
    indexColVal.put(Bytes.toBytes((short) indexRegionStartKey.length));
    indexColVal.put(Bytes.toBytes(rowKeyOffset));
    Put idxPut = new Put(indexRowKey.array());
    idxPut.add(Constants.IDX_COL_FAMILY, Constants.IDX_COL_QUAL, tsForIndexTabPut,
      indexColVal.array());
    idxPut.setDurability(Durability.SKIP_WAL);
    return idxPut;
  }

  private static Cell selectKVForIndexing(List<Cell> values) {
    Cell kv = null;
    long ts = HConstants.OLDEST_TIMESTAMP;
    for (Cell value : values) {
      // When the TS is same, then we need to consider the last KV
      // appearing in the KVList
      // as this will be added to the memstore with highest memstore TS.
      if (value.getTimestamp() >= ts) {
        kv = value;
        ts = value.getTimestamp();
      }
    }
    return kv;
  }

  public static ByteArrayBuilder getIndexRowKeyHeader(IndexSpecification index,
      byte[] indexRegionStartKey, byte[] primaryRowKey) {
    /*
     * Format for the rowkey for index table [Startkey for the index region] + [one 0 byte] + [Index
     * name] + [Padding for the max index name] + [[index col value]+[padding for the max col value]
     * for each of the index col] + [user table row key] To know the reason for adding empty byte
     * array refert to HDP-1666
     */
    byte[] indexName = Bytes.toBytes(index.getName());
    int totalValueLength = index.getTotalValueLength();
    int maxIndexNameLength = IndexUtils.getMaxIndexNameLength();
    int rowLength =
        indexRegionStartKey.length + maxIndexNameLength + totalValueLength + primaryRowKey.length
            + 1;
    ByteArrayBuilder row = ByteArrayBuilder.allocate(rowLength);

    // STEP 1 : Adding the startkey for the index region and single empty Byte.
    row.put(indexRegionStartKey);
    // one byte [0] to be added after the index region startkey. This is for the case of
    // entries added to the 1st region.Here the startkey of the region will be empty byte[]
    // So the 1st byte(s) which comes will be the index name and it might not fit into the
    // 1st region [As per the end key of that region]
    // Well all the bytes in the byte[] 'row' will be 0s already. No need to put a 0 byte
    // Just need to advance the position by 1
    row.position(row.position() + 1);

    // STEP 2 : Adding the index name and the padding needed
    row.put(indexName);
    int padLength = maxIndexNameLength - indexName.length;
    // Well all the bytes in the byte[] 'row' will be 0s already. No need to put a 0 padding bytes
    // Just need to advance the position by padLength
    row.position(row.position() + padLength);
    return row;
  }

  public static void updateRowKeyForKV(ColumnQualifier indexCQ, Cell kv,
      ByteArrayBuilder indexRowKey) throws IOException {
    byte[] value = getValueFromKV(kv, indexCQ);
    int valuePadLength = indexCQ.getMaxValueLength() - value.length;
    if (valuePadLength < 0) {
      String errMsg =
          "The value length for the column " + indexCQ.getColumnFamilyString() + ":"
              + indexCQ.getQualifierString() + " is greater than the cofigured max value length : "
              + indexCQ.getMaxValueLength();
      LOG.warn(errMsg);
      throw new IOException(errMsg);
    }
    indexRowKey.put(value);
    indexRowKey.position(indexRowKey.position() + valuePadLength);
  }

  private static byte[] getValueFromKV(Cell kv, ColumnQualifier indexCQ) {
    ValuePartition vp = indexCQ.getValuePartition();
    byte value[] = null;
    if (vp != null) {
      value = vp.getPartOfValue(kv.getValue());
      if (value != null) {
        value = IndexUtils.changeValueAccToDataType(value, indexCQ.getType());
      }
    } else {
      LOG.trace("No offset or separator is mentioned. So just returning the value fetched from kv");
      value = kv.getValue();
      value = IndexUtils.changeValueAccToDataType(value, indexCQ.getType());
    }
    return value;
  }

  public static byte[] getRowKeyFromKV(Cell kv) {
    byte[] row = kv.getRow();
    // Row key of the index table entry = region startkey + index name + column value(s)
    // + actual table rowkey.
    // Every row in the index table will have exactly one KV in that. The value will be
    // 4 bytes. First 2 bytes specify length of the region start key bytes part in the
    // rowkey. Last 2 bytes specify the offset to the actual table rowkey part within the
    // index table rowkey.
    byte[] value = kv.getValue();
    short actualRowKeyOffset = Bytes.toShort(value, 2);
    byte[] actualTableRowKey = new byte[row.length - actualRowKeyOffset];
    System.arraycopy(row, actualRowKeyOffset, actualTableRowKey, 0, actualTableRowKey.length);
    return actualTableRowKey;
  }

  public static void createIndexTable(String userTable, Configuration conf,
      Map<String, List<String>> indexColumnFamily) throws IOException, InterruptedException,
      ClassNotFoundException {
    HBaseAdmin hbaseAdmin = new IndexAdmin(conf);

    try {
      HTableDescriptor tableDescriptor = hbaseAdmin.getTableDescriptor(Bytes.toBytes(userTable));

      String input = conf.get(TABLE_INPUT_COLS);

      HTableDescriptor ihtd = parse(userTable, tableDescriptor, input, indexColumnFamily);

      // disable the table
      hbaseAdmin.disableTable(userTable);
      // This will create the index table. Also modifies the existing table htable descriptor.
      hbaseAdmin.modifyTable(Bytes.toBytes(userTable), ihtd);
      hbaseAdmin.enableTable(Bytes.toBytes(userTable));
    } finally {
      if (hbaseAdmin != null) {
        hbaseAdmin.close();
      }
    }
  }

  // This can be a comma seperated list
  // We can pass like
  // IDX1=>cf1:[q1->datatype&
  // length],[q2],[q3];cf2:[q1->datatype&length],[q2->datatype&length],[q3->datatype&
  // lenght]#IDX2=>cf1:q5,q5
  public static HTableDescriptor parse(String tableNameToIndex, HTableDescriptor tableDesc,
      String input, Map<String, List<String>> cfs) throws IOException {
    List<String> colFamilyList = new ArrayList<String>();

    for (HColumnDescriptor hColumnDescriptor : tableDesc.getColumnFamilies()) {
      colFamilyList.add(hColumnDescriptor.getNameAsString());
    }
    TableIndices indices = new TableIndices();
    if (input != null) {
      String[] indexSplits = input.split("#");
      for (String index : indexSplits) {
        String[] indexName = index.split("=>");
        if (indexName.length < 2) {
          System.out.println("Invalid entry.");
          System.exit(-1);
        }
        IndexSpecification iSpec = new IndexSpecification(indexName[0]);

        String[] cfSplits = indexName[1].split(";");
        if (cfSplits.length < 1) {
          System.exit(-1);
        } else {
          for (String cf : cfSplits) {
            String[] qualSplits = cf.split(":");
            if (qualSplits.length < 2) {
              System.out.println("The qualifiers are not given");
              System.exit(-1);
            }
            if (!colFamilyList.contains(qualSplits[0])) {
              System.out.println("Valid CF not found");
              System.exit(-1);
            }
            String[] qualDetails = qualSplits[1].split(",");
            for (String details : qualDetails) {
              String substring = details.substring(1, details.lastIndexOf("]"));
              if (substring != null) {
                String[] splitQual = substring.split("->");
                if (splitQual.length < 2) {
                  System.out.println("Default value length and data type will be take");
                  iSpec.addIndexColumn(new HColumnDescriptor(qualSplits[0]), splitQual[0],
                    ValueType.String, Constants.DEF_MAX_INDEX_NAME_LENGTH);
                } else {
                  String[] valueType = splitQual[1].split("&");
                  iSpec.addIndexColumn(new HColumnDescriptor(qualSplits[0]), splitQual[0],
                    ValueType.valueOf(valueType[0]), Integer.parseInt(valueType[1]));
                }
                if (cfs != null) {
                  addToMap(cfs, qualSplits, splitQual);
                }
              }
            }
          }
        }
        indices.addIndex(iSpec);
      }
    }
    tableDesc.setValue(Constants.INDEX_SPEC_KEY, indices.toByteArray());
    return tableDesc;
  }

  private static void addToMap(Map<String, List<String>> cfs, String[] qualSplits,
      String[] splitQual) {
    if (cfs.get(qualSplits[0]) == null) {
      List<String> qual = new ArrayList<String>();
      qual.add(splitQual[0]);
      cfs.put(qualSplits[0], qual);
    } else {
      List<String> list = cfs.get(qualSplits[0]);
      list.add(splitQual[0]);
    }
  }

}
