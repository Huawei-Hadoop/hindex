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
package org.apache.hadoop.hbase.index.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.index.SeparatorPartition;
import org.apache.hadoop.hbase.index.SpatialPartition;
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.index.ValuePartition.PartitionType;

/**
 * This filter is used to filter cells based on a part of it's value. It takes a
 * {@link CompareFilter.CompareOp} operator (equal, greater, not equal, etc), and either a byte []
 * value or a WritableByteArrayComparable.
 * <p>
 * You must also specify a family and qualifier. Only the value of this column will be tested. When
 * using this filter on a {@link Scan} with specified inputs, the column to be tested should also be
 * added as input (otherwise the filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found on a row, use
 * {@link #setFilterIfMissing}. Otherwise, if the column is found, the entire row will be emitted
 * only if the value passes. If the value fails, the row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set {@link #setLatestVersionOnly} to
 * false. The default is true, meaning that only the latest version's value is tested and all
 * previous versions are ignored.
 */
public class SingleColumnValuePartitionFilter extends SingleColumnValueFilter {

  private boolean foundColumn = false;
  private boolean matchedColumn = false;
  private ValuePartition valuePartition = null;

  /**
   * Writable constructor, do not use.
   */
  public SingleColumnValuePartitionFilter() {
  }

  public SingleColumnValuePartitionFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final byte[] value, ValuePartition vp) {
    this(family, qualifier, compareOp, new BinaryComparator(value), vp);
  }

  public SingleColumnValuePartitionFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator, ValuePartition vp) {
    super(family, qualifier, compareOp, comparator);
    this.valuePartition = vp;
  }

  public ValuePartition getValuePartition() {
    return valuePartition;
  }

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn ? !this.matchedColumn : this.getFilterIfMissing();
  }

  public void reset() {
    foundColumn = false;
    matchedColumn = false;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    if (this.matchedColumn) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.getLatestVersionOnly() && this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    byte[] value = valuePartition.getPartOfValue(keyValue.getValue());
    if (filterColumnValue(value, 0, value.length)) {
      return this.getLatestVersionOnly() ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final byte[] data, final int offset, final int length) {
    int compareResult = this.getComparator().compareTo(data, offset, length);
    switch (this.getOperator()) {
    case LESS:
      return compareResult <= 0;
    case LESS_OR_EQUAL:
      return compareResult < 0;
    case EQUAL:
      return compareResult != 0;
    case NOT_EQUAL:
      return compareResult == 0;
    case GREATER_OR_EQUAL:
      return compareResult > 0;
    case GREATER:
      return compareResult >= 0;
    default:
      throw new RuntimeException("Unknown Compare op " + this.getOperator().name());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(valuePartition.getPartitionType().name());
    valuePartition.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    PartitionType p = PartitionType.valueOf(in.readUTF());
    if (p.equals(PartitionType.SEPARATOR)) {
      valuePartition = new SeparatorPartition();
    } else if (p.equals(PartitionType.SPATIAL)) {
      valuePartition = new SpatialPartition();
    }
    if (valuePartition != null) {
      valuePartition.readFields(in);
    }
  }
}
