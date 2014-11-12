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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.mortbay.log.Log;


/**
 * An IndexSpecification should have a unique name and can be created on 1 or more columns (Here
 * column refers to columnfamily + qualifier) For each of such column a ColumnQualifier is provided
 * which takes the column details. This includes the column family name and qualifier name. The
 * additional columns are those columns in the main table whose value also will be captured in the
 * secondary index table.
 * Index Specfication name should not start with '.' and '-'.
 * Can contain alphanumerics and '-','.','_'.
 */

public class IndexSpecification implements WritableComparable<IndexSpecification> {

  public static final String ARBITRARY_COL_IDX_NAME = "ARB_COL_IDX";
  public static final byte[] ARBITRARY_COL_IDX_NAME_BYTES = Bytes.toBytes(ARBITRARY_COL_IDX_NAME);
  public static final String ARBITRARY_COL_NAME = "ARB_COL";
  public static final byte[] ARBITRARY_COL_NAME_BYTES = Bytes.toBytes(ARBITRARY_COL_NAME);

  private byte[] name;

  private Set<ColumnQualifier> indexColumns = new LinkedHashSet<ColumnQualifier>(1);

  private int totalValueLength = 0;

  private long ttl = -1;

  private int maxVersions = -1;

  // Empty constructor for serialization and deserialization.
  public IndexSpecification() {
  }

  /**
   * @param name should not start with '.' and '-'. Can contain alphanumerics and '-','.','_'.
   * @throws IllegalArgumentException if invalid table name is provided
   */
  public IndexSpecification(String name) {
    validateIndexSpecification(Bytes.toBytes(name));
    this.name = Bytes.toBytes(name);
  }

  private void validateIndexSpecification(byte[] indexSpecName) {
    // throws IllegalArgException if invalid index name is provided
    IndexUtils.isLegalIndexName(indexSpecName);
  }

  /**
   * @param name should not start with '.' and '-'. Can contain alphanumerics and '-','.','_'.
   * @throws IllegalArgumentException if invalid table name is provided
   */
  public IndexSpecification(byte[] name) {
    validateIndexSpecification(name);
    this.name = name;
  }

  /**
   * @return index name
   */
  public String getName() {
    return Bytes.toString(this.name);
  }

  /**
   * @param cf column family
   * @param qualifier
   * @param type - If type is specified as null then by default ValueType will be taken as String.
   * @param maxValueLength
   * @throws IllegalArgumentException If column family name and/or qualifier is null or blanks.<br/>
   *           If column family name starts with '.',contains control characters or colons.
   * @see ValueType
   */
  public void addIndexColumn(HColumnDescriptor cf, String qualifier, ValueType type,
      int maxValueLength) throws IllegalArgumentException {
    type = checkForType(type);
    isValidFamilyAndQualifier(cf, qualifier);
    maxValueLength = getMaxLength(type, maxValueLength);
    ColumnQualifier cq = new ColumnQualifier(cf.getNameAsString(), qualifier, type, maxValueLength);
    isNotDuplicateEntry(cq);
    formMinTTL(cf);
    formMaxVersions(cf);
    internalAdd(cq);
  }

  private ValueType checkForType(ValueType type) {
    if (type == null) {
      type = ValueType.String;
    }
    return type;
  }

  private int getMaxLength(ValueType type, int maxValueLength) {
    if ((type == ValueType.Int || type == ValueType.Float) && maxValueLength != 4) {
      Log.warn("With integer or float datatypes, the maxValueLength has to be 4 bytes");
      return 4;
    }
    if ((type == ValueType.Double || type == ValueType.Long) && maxValueLength != 8) {
      Log.warn("With Double and Long datatypes, the maxValueLength has to be 8 bytes");
      return 8;
    }
    if ((type == ValueType.Short || type == ValueType.Char) && maxValueLength != 2) {
      Log.warn("With Short and Char datatypes, the maxValueLength has to be 2 bytes");
      return 2;
    }
    if (type == ValueType.Byte && maxValueLength != 1) {
      Log.warn("With Byte datatype, the maxValueLength has to be 1 bytes");
      return 1;
    }
    if (type == ValueType.String && maxValueLength == 0) {
      Log.warn("With String datatype, the minimun value length is 2");
      maxValueLength = 2;
    }
    return maxValueLength;
  }

  /**
   * @param cf Column Family
   * @param qualifier Column Qualifier
   * @param vp Value Partition
   * @param type Data Type
   * @param maxValueLength
   * @throws IllegalArgumentException
   */
  public void addIndexColumn(HColumnDescriptor cf, String qualifier, ValuePartition vp,
      ValueType type, int maxValueLength) throws IllegalArgumentException {
    checkForType(type);
    isValidFamilyAndQualifier(cf, qualifier);
    maxValueLength = getMaxLength(type, maxValueLength);
    ColumnQualifier cq =
        new ColumnQualifier(cf.getNameAsString(), qualifier, type, maxValueLength, vp);
    isNotDuplicateEntry(cq);
    formMinTTL(cf);
    formMaxVersions(cf);
    internalAdd(cq);
  }

  private void formMinTTL(HColumnDescriptor cf) {
    int timeToLive = cf.getTimeToLive();
    if (ttl == -1) {
      ttl = timeToLive;
    } else if (timeToLive != HConstants.FOREVER && timeToLive != -1) {
      if (timeToLive < ttl) {
        ttl = timeToLive;
      }
    }
  }

  private void formMaxVersions(HColumnDescriptor cf) {
    int maxVersion = cf.getMaxVersions();
    if (maxVersions == -1) {
      maxVersions = maxVersion;
    } else if (maxVersion != HConstants.FOREVER && maxVersion != -1) {
      if (maxVersion < maxVersions) {
        maxVersions = maxVersion;
      }
    }
  }

  private void internalAdd(ColumnQualifier cq) {
    indexColumns.add(cq);
    totalValueLength += cq.getMaxValueLength();
  }

  /**
   * @return List of column specifiers
   */
  public Set<ColumnQualifier> getIndexColumns() {
    return this.indexColumns;
  }

  /**
   * @param cf column family
   * @param qualifier
   * @throws IllegalArgumentException If column family name and/or qualifier is null or blanks
   * @throws IllegalArgumentException If column family name starts with '.',contains control
   *           characters or colons
   */
  private static void isValidFamilyAndQualifier(HColumnDescriptor cf, String qualifier) {
    isValidFamily(cf);
    if (null == qualifier || StringUtils.isBlank(qualifier)) {
      throw new IllegalArgumentException("Column qualifier should not be null/empty.");
    }
  }

  private static void isValidFamily(HColumnDescriptor cf) {
    if (null == cf || StringUtils.isBlank(cf.getNameAsString())) {
      throw new IllegalArgumentException("Column family should not be null/empty.");
    }
  }

  /**
   * @param ColumnQualifier to check duplicate entry
   */
  private void isNotDuplicateEntry(ColumnQualifier c) {
    if (this.getIndexColumns().contains(c)) {
      throw new IllegalArgumentException("Duplicate column family and qualifier "
          + "combination should not be present.");
    }
  }

  /**
   * @param in Input Stream
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException {
    this.name = Bytes.readByteArray(in);
    int indexColsSize = in.readInt();
    indexColumns.clear();
    for (int i = 0; i < indexColsSize; i++) {
      ColumnQualifier cq = new ColumnQualifier();
      // Need to revisit this place. May be some other valid value though invalid
      // comes up.
      try {
        cq.readFields(in);
      } catch (IllegalArgumentException e) {
        throw new EOFException("Received unexpected data while parsing the column qualifiers.");
      }
      internalAdd(cq);
    }
    this.maxVersions = in.readInt();
    this.ttl = in.readLong();
  }

  /**
   * @param out Output Stream
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.name);
    out.writeInt(this.indexColumns.size());
    for (ColumnQualifier cq : this.indexColumns) {
      cq.write(out);
    }
    out.writeInt(maxVersions);
    out.writeLong(ttl);
  }

  /**
   * @param iSpec
   * @return int
   */
  public int compareTo(IndexSpecification iSpec) {
    return Bytes.compareTo(this.name, Bytes.toBytes(iSpec.getName()));
  }

  public String toString() {
    return "Index : " + getName() + ",Index Columns : " + indexColumns;
  }

  public boolean equals(Object obj) {
    if (obj instanceof IndexSpecification) {
      IndexSpecification other = (IndexSpecification) obj;
      return Bytes.equals(this.name, other.name);
    }
    return false;
  }

  public int hashCode() {
    return Bytes.hashCode(this.name);
  }

  public boolean contains(byte[] family) {
    for (ColumnQualifier qual : indexColumns) {
      if (Bytes.equals(family, qual.getColumnFamily())) {
        return true;
      }
    }
    return false;
  }

  public boolean contains(byte[] family, byte[] qualifier) {
    if (qualifier == null || qualifier.length == 0) {
      return contains(family);
    }
    for (ColumnQualifier qual : indexColumns) {
      if (Bytes.equals(family, qual.getColumnFamily())
          && Bytes.equals(qualifier, qual.getQualifier())) {
        return true;
      }
    }
    return false;
  }

  public int getTotalValueLength() {
    return totalValueLength;
  }

  /**
   * Return the minimum of the timeToLive specified for the column families in the specifed index.
   * @return ttl
   */
  public long getTTL() {
    return this.ttl;
  }

  /**
   * Return the minimum of the maxVersion specified for the column families in the specified index.
   * @return max versions.
   */
  public int getMaxVersions() {
    return this.maxVersions;
  }

  public IndexSpecification deepCopy() {
    IndexSpecification dup = new IndexSpecification();
    dup.name = this.name;
    dup.totalValueLength = this.totalValueLength;
    dup.ttl = this.ttl;
    dup.maxVersions = this.maxVersions;
    dup.indexColumns = new LinkedHashSet<ColumnQualifier>(this.indexColumns.size());
    dup.indexColumns.addAll(this.indexColumns);
    return dup;
  }

  /**
   * Normal created indexes needed full column name to be specified ie. cf:q <br>
   * HBase allows to write using arbitrary qualifier names. This index helps to index these
   * arbitrary columns. HIndex will index each of the qualifier in the specified cfs with entry for
   * each one of the qulifier into index table. There can be only one such arbitrary index on a
   * table nd need to pass all the cfs to be indexed, to this call. When there is an arbitrary index
   * on a table, we dont allow any other index to be specified on this table.
   * @param cols
   * @return
   */
  public static IndexSpecification createArbitraryColumnIndex(HColumnDescriptor... cols) {
    IndexSpecification idxSpec = new IndexSpecification();
    idxSpec.name = ARBITRARY_COL_IDX_NAME_BYTES;
    for (HColumnDescriptor col : cols) {
      isValidFamily(col);
      idxSpec.formMinTTL(col);
      idxSpec.formMaxVersions(col);
      ColumnQualifier cq = new ColumnQualifier(col.getNameAsString(), ARBITRARY_COL_NAME,
          ValueType.String, -1);
      idxSpec.indexColumns.add(cq);
    }
    return idxSpec;
  }
}
