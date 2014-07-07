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
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterColumnValueRange extends FilterColumnValueDetail {
  private CompareOp upperBoundCompareOp;
  private byte[] upperBoundValue;

  public FilterColumnValueRange(byte[] cf, byte[] qualifier, byte[] lowerBoundValue,
      CompareOp lowerBoundCompareOp, byte[] upperBoundValue, CompareOp upperBoundCompareOp) {
    super(cf, qualifier, lowerBoundValue, lowerBoundCompareOp);
    this.upperBoundCompareOp = upperBoundCompareOp;
    this.upperBoundValue = upperBoundValue;
  }

  public FilterColumnValueRange(byte[] cf, byte[] qualifier, ValuePartition vp,
      byte[] lowerBoundValue, CompareOp lowerBoundCompareOp, byte[] upperBoundValue,
      CompareOp upperBoundCompareOp) {
    super(cf, qualifier, lowerBoundValue, vp, lowerBoundCompareOp);
    this.upperBoundCompareOp = upperBoundCompareOp;
    this.upperBoundValue = upperBoundValue;
  }

  public FilterColumnValueRange(Column column, byte[] lowerBoundValue,
      CompareOp lowerBoundCompareOp, byte[] upperBoundValue, CompareOp upperBoundCompareOp) {
    super(column, lowerBoundValue, lowerBoundCompareOp);
    this.upperBoundCompareOp = upperBoundCompareOp;
    this.upperBoundValue = upperBoundValue;
  }

  // No need to have the hashCode() and equals(Object obj) implementation here. Super class
  // implementation checks for the CF name and qualifier name which is sufficient.

  public String toString() {
    return String.format("%s (%s, %s, %s, %s, %s, %s, %s)", this.getClass().getSimpleName(), Bytes
        .toStringBinary(this.cf), Bytes.toStringBinary(this.qualifier), this.valueType.name(),
      this.compareOp.name(), Bytes.toStringBinary(this.value),
      this.upperBoundCompareOp == null ? "" : this.upperBoundCompareOp.name(),
      this.upperBoundValue == null ? "" : Bytes.toStringBinary(this.upperBoundValue));
  }

  public CompareOp getUpperBoundCompareOp() {
    return this.upperBoundCompareOp;
  }

  public byte[] getUpperBoundValue() {
    return this.upperBoundValue;
  }

  // The equals method is a bit tricky.
  // Needs one more eye on this
  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    FilterColumnValueRange fcvr = null;
    if (other instanceof FilterColumnValueRange && super.equals(other)) {
      fcvr = (FilterColumnValueRange) other;
      if (this.upperBoundValue != null && fcvr.getUpperBoundValue() != null) {
        if (Bytes.compareTo(this.upperBoundValue, fcvr.getUpperBoundValue()) != 0) {
          return false;
        }
      } else if (this.upperBoundValue == null && fcvr.getUpperBoundValue() == null) {
        return true;
      } else {
        return false;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}