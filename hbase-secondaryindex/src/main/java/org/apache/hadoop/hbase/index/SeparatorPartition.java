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
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A column value is composed of many values separated using some known separator.
 * Part of the column value to be indexed. This class specified how to get that value part.
 * Takes the separator so as to split the value and the value position in the split. Note that 
 * the position index starts from '1'
 */
public class SeparatorPartition extends ValuePartition {

  private byte[] separator;

  private int position;

  public SeparatorPartition(String separator, int position) {
    if ((null == separator || separator.length() == 0)) {
      throw new IllegalArgumentException("Separator cannot be null");
    }
    if ((null != separator) && position == 0) {
      throw new IllegalArgumentException("With separator ,the position cannot be zero.");
    }
    this.separator = Bytes.toBytes(separator);
    this.position = position;
  }

  public SeparatorPartition(byte[] separator, int position) {
    this.separator = separator;
    this.position = position;
  }

  @Override
  public PartitionType getPartitionType() {
    return PartitionType.SEPARATOR;
  }

  public byte[] getSeparator() {
    return this.separator;
  }

  public int getPosition() {
    return this.position;
  }

  @Override
  public byte[] getPartOfValue(byte[] value) {
    // TODO check this method.. Seems so much of code!
    int sepLastKnownPosition = -1;
    int sepCurrPositon = -1;
    int separatorOccurences = 0;
    byte[] kvSubset = new byte[separator.length];
    for (int i = 0; i < value.length;) {
      if ((value.length - i) >= separator.length) {
        System.arraycopy(value, i, kvSubset, 0, separator.length);
        if (Bytes.equals(kvSubset, separator)) {
          separatorOccurences++;
          sepLastKnownPosition = sepCurrPositon;
          sepCurrPositon = i;
          i += separator.length;
        } else {
          i++;
        }
        if (separatorOccurences < this.position) {
          continue;
        }
        break;
      }
      break;
    }
    if (separatorOccurences < this.position - 1) {
      return new byte[0];
    }
    byte valuePart[] = null;
    if (separatorOccurences == this.position - 1) {
      if (sepCurrPositon == -1) {
        valuePart = value;
      } else {
        valuePart = new byte[value.length - sepCurrPositon - separator.length];
        System.arraycopy(value, sepCurrPositon + separator.length, valuePart, 0, valuePart.length);
      }
      return valuePart;
    } else if (separatorOccurences == this.position) {
      if (sepLastKnownPosition == -1) {
        valuePart = new byte[sepCurrPositon];
        System.arraycopy(value, 0, valuePart, 0, valuePart.length);
      } else {
        valuePart = new byte[sepCurrPositon - sepLastKnownPosition - separator.length];
        System.arraycopy(value, sepLastKnownPosition + separator.length, valuePart, 0,
          valuePart.length);
      }
      return valuePart;
    }
    return valuePart;
  }

  @Override
  public ValuePartitionProtos.ValuePartition convert() {
    Builder builder = ValuePartitionProtos.ValuePartition.newBuilder();
    builder.setPartitionType(PartitionType.SEPARATOR);
    builder
        .setExtension(
          org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.separator,
          ByteString.copyFrom(this.separator));
    builder
        .setExtension(
          org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.position,
          this.position);
    return builder.build();
  }

  @Override
  public int compareTo(ValuePartition vp) {
    if (!(vp instanceof SeparatorPartition)) return 1;
    SeparatorPartition sp = (SeparatorPartition) vp;
    int diff = Bytes.compareTo(this.separator, sp.separator);
    if (diff == 0) return this.position - sp.position;
    return diff;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that instanceof SeparatorPartition) {
      SeparatorPartition sp = (SeparatorPartition) that;
      return Bytes.compareTo(this.separator, sp.getSeparator()) == 0
          && this.position == sp.getPosition();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 13;
    result ^= Bytes.hashCode(this.separator);
    result ^= this.position;
    return result;
  }

  public static ValuePartition parseFrom(final byte[] bytes) throws DeserializationException {
    ValuePartitionProtos.ValuePartition valuePartition;
    try {
      ExtensionRegistry registry = ExtensionRegistry.newInstance();
      registry
          .add(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.separator);
      registry
          .add(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.position);
      valuePartition = ValuePartitionProtos.ValuePartition.parseFrom(bytes, registry);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new SeparatorPartition(
        valuePartition
            .getExtension(
              org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.separator)
            .toByteArray(),
        valuePartition
            .getExtension(org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.SeparatorPartition.position));
  }

  @Override
  public byte[] toByteArray() {
    return convert().toByteArray();
  }
}
