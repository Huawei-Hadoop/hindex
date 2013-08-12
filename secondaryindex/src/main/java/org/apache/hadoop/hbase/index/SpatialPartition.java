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
package org.apache.hadoop.hbase.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Column value is composed of different values. The value to be indexed is in a known offset
 */
public class SpatialPartition implements ValuePartition {

  private static final long serialVersionUID = 4154246616417056121L;

  private int offset;

  private int length;

  public SpatialPartition() {

  }

  public SpatialPartition(int offset, int length) {
    if (offset < 0 || length < 1) {
      throw new IllegalArgumentException("offset/length cannot be les than 1");
    }
    this.offset = offset;
    this.length = length;
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.SPATIAL;
  }

  @Override
  public byte[] getPartOfValue(byte[] value) {
    if (this.offset >= value.length) {
      return new byte[0];
    } else {
      int valueLength =
          (this.offset + this.length > value.length) ? value.length - this.offset : this.length;
      byte[] valuePart = new byte[valueLength];
      System.arraycopy(value, this.offset, valuePart, 0, valueLength);
      return valuePart;
    }
  }

  @Override
  public int compareTo(ValuePartition vp) {
    if (!(vp instanceof SpatialPartition)) return 1;
    SpatialPartition sp = (SpatialPartition) vp;
    int diff = this.offset - sp.offset;
    if (diff == 0) return this.length - sp.length;
    return diff;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that instanceof SpatialPartition) {
      SpatialPartition sp = (SpatialPartition) that;
      return this.offset == sp.getOffset() && this.length == sp.getLength();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 13;
    result ^= this.offset;
    result ^= this.length;
    return result;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.offset);
    out.writeInt(this.length);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.offset = in.readInt();
    this.length = in.readInt();
  }
}
