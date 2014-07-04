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


import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos;
import org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.ValuePartition.Builder;
import org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.ValuePartition.PartitionType;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Column value is composed of different values. The value to be indexed is in a known offset
 */
public class SpatialPartition extends ValuePartition {

  private int offset;

  private int length;

  public SpatialPartition(int offset, int length) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset/length cannot be les than 0");
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
  public ValuePartitionProtos.ValuePartition convert() {
    Builder builder = ValuePartitionProtos.ValuePartition.newBuilder();
    builder.setPartitionType(PartitionType.SPATIAL);
    builder
        .setExtension(
          org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.length,
          this.length);
    builder
        .setExtension(
          org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.offset,
          this.offset);
    return builder.build();
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

  public static ValuePartition parseFrom(final byte[] bytes) throws DeserializationException {
    ValuePartitionProtos.ValuePartition valuePartition = null;
    try {
      ExtensionRegistry registry = ExtensionRegistry.newInstance();
      registry
          .add(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.offset);
      registry
          .add(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.length);
      valuePartition =
          ValuePartitionProtos.ValuePartition.newBuilder().mergeFrom(bytes, registry).build();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new SpatialPartition(
        valuePartition
            .getExtension(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.offset),
        valuePartition
            .getExtension(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SpatialPartition.length));
  }

  @Override
  public byte[] toByteArray() {
    return convert().toByteArray();
  }
}
