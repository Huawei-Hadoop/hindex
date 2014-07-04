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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterColumnValueDetail {
  protected byte[] cf;
  protected byte[] qualifier;
  protected byte[] value;
  protected CompareOp compareOp;
  protected Column column;
  protected int maxValueLength;
  protected ValueType valueType;

  public FilterColumnValueDetail(byte[] cf, byte[] qualifier, byte[] value, CompareOp compareOp) {
    this.cf = cf;
    this.qualifier = qualifier;
    this.value = value;
    this.compareOp = compareOp;
    this.column = new Column(this.cf, this.qualifier);
  }

  public FilterColumnValueDetail(byte[] cf, byte[] qualifier, byte[] value,
      ValuePartition valuePartition, CompareOp compareOp) {
    this.cf = cf;
    this.qualifier = qualifier;
    this.value = value;
    this.compareOp = compareOp;
    this.column = new Column(this.cf, this.qualifier, valuePartition);
  }

  public FilterColumnValueDetail(Column column, byte[] value, CompareOp compareOp) {
    this.cf = column.getFamily();
    this.qualifier = column.getQualifier();
    this.value = value;
    this.compareOp = compareOp;
    this.column = column;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof FilterColumnValueDetail)) return false;
    FilterColumnValueDetail that = (FilterColumnValueDetail) obj;
    if (!this.column.equals(that.column)) {
      return false;
    }
    // Need to check.
    if (this.value != null && that.value != null) {
      if (!(Bytes.equals(this.value, that.value))) return false;
    } else if (this.value == null && that.value == null) {
      return true;
    } else {
      return false;
    }
    return true;
  }

  public int hashCode() {
    return this.column.hashCode();
  }

  public String toString() {
    return String.format("%s (%s, %s, %s, %s, %s)", this.getClass().getSimpleName(),
      Bytes.toStringBinary(this.cf), Bytes.toStringBinary(this.qualifier), this.valueType.name(),
      this.compareOp.name(), Bytes.toStringBinary(this.value));
  }

  public Column getColumn() {
    return this.column;
  }

  public byte[] getValue() {
    return this.value;
  }

  protected void setValue(byte[] value) {
    this.value = value;
  }

}