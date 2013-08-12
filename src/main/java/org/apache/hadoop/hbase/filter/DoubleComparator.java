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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;

public class DoubleComparator extends DecimalComparator {
  public DoubleComparator() {
  }

  public DoubleComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte[] actualValue, int offset, int length) {
    ByteArrayBuilder val = new ByteArrayBuilder(length);
    val.put(actualValue, offset, length);
    byte[] array = val.array();
    if (msb[0] == 0) {
      value[0] ^= (1 << 7);
      array[0] ^= (1 << 7);
    } else {
      for (int i = 0; i < 8; i++) {
        value[i] ^= 0xff;
      }

      for (int i = 0; i < 8; i++) {
        array[i] ^= 0xff;
      }
    }
    int compareTo = super.compareTo(array, 0, length);
    System.arraycopy(temp, 0, value, 0, value.length);
    return compareTo;
  }
}
