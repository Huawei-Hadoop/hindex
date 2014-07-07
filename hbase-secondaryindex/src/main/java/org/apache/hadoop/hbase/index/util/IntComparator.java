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

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;

import com.google.protobuf.InvalidProtocolBufferException;

public class IntComparator extends DecimalComparator {

  public IntComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte[] actualValue, int offset, int length) {
    ByteArrayBuilder val = new ByteArrayBuilder(length);
    val.put(actualValue, offset, length);
    value[0] ^= (1 << 7);
    byte[] array = val.array();
    array[0] ^= (1 << 7);
    int compareTo = super.compareTo(array, 0, length);
    System.arraycopy(temp, 0, value, 0, value.length);
    return compareTo;
  }

  /**
   * @param pbBytes A pb serialized {@link IntComparator} instance
   * @return An instance of {@link IntComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static IntComparator parseFrom(final byte[] pbBytes) throws DeserializationException {
    ComparatorProtos.BinaryComparator proto;
    try {
      proto = ComparatorProtos.BinaryComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new IntComparator(proto.getComparable().getValue().toByteArray());
  }

}
