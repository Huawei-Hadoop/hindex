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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * A column value is composed of many values separated using some known separator. Part of the
 * column value to be indexed. This class specified how to get that value part. Takes the separator
 * so as to split the value and the value position in the split. Note that the position index starts
 * from '1'
 */
public class SeparatorPartition implements ValuePartition {

  private static final long serialVersionUID = -3409814164480687975L;

  private byte[] separator;

  private int position;

  public SeparatorPartition() {

  }

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
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.separator);
    out.writeInt(this.position);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.separator = Bytes.readByteArray(in);
    this.position = in.readInt();
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
}
