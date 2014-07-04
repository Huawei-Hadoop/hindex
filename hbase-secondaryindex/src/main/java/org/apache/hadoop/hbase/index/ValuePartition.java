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

import java.io.Serializable;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos;
import org.apache.hadoop.hbase.index.protobuf.generated.ValuePartitionProtos.ValuePartition.PartitionType;

/**
 * Used to specify the part of a column which needs to be indexed.
 */
public abstract class ValuePartition implements Comparable<ValuePartition>, Serializable {

  private static final long serialVersionUID = -3409814164480687975L;

  public abstract PartitionType getPartitionType();

  public abstract byte[] getPartOfValue(byte[] value);

  public abstract byte[] toByteArray();

  public abstract ValuePartitionProtos.ValuePartition convert();

  public static ValuePartition parseFrom(final byte[] bytes) throws DeserializationException {
    throw new DeserializationException(
        "parseFrom called on base ValuePartition, but should be called on derived type");
  }

}
