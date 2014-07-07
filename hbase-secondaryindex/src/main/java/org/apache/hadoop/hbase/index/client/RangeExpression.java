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

import java.io.Serializable;

import org.apache.hadoop.hbase.index.Column;

/**
 * Can be used to specify a range condition on a column associated with an index. When the range is
 * non closed at one end (to specific upper bound but only lower bound) pass the corresponding bound
 * value as null.
 */
public class RangeExpression implements Serializable {

  private static final long serialVersionUID = 8772267632040419734L;

  private Column column;
  private byte[] lowerBoundValue;
  private byte[] upperBoundValue;
  private boolean lowerBoundInclusive;
  private boolean upperBoundInclusive;

  public Column getColumn() {
    return column;
  }

  public byte[] getLowerBoundValue() {
    return lowerBoundValue;
  }

  public byte[] getUpperBoundValue() {
    return upperBoundValue;
  }

  public boolean isLowerBoundInclusive() {
    return lowerBoundInclusive;
  }

  public boolean isUpperBoundInclusive() {
    return upperBoundInclusive;
  }

  /**
   * When the range is non closed at one end (to specific upper bound but only lower bound) pass the
   * corresponding bound value as null.
   * @param column
   * @param lowerBoundValue
   * @param upperBoundValue
   * @param lowerBoundInclusive
   * @param upperBoundInclusive
   */
  public RangeExpression(Column column, byte[] lowerBoundValue, byte[] upperBoundValue,
      boolean lowerBoundInclusive, boolean upperBoundInclusive) {
    if (column == null || (lowerBoundValue == null && upperBoundValue == null)) {
      throw new IllegalArgumentException();
    }
    this.column = column;
    this.lowerBoundValue = lowerBoundValue;
    this.upperBoundValue = upperBoundValue;
    this.lowerBoundInclusive = lowerBoundInclusive;
    this.upperBoundInclusive = upperBoundInclusive;
  }

  @Override
  public String toString() {
    return "RangeExpression : Column[" + this.column + "], lowerBoundInclusive : "
        + this.lowerBoundInclusive + ", upperBoundInclusive : " + this.upperBoundInclusive;
  }
}
