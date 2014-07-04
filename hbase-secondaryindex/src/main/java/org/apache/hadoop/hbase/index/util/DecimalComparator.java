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


import org.apache.hadoop.hbase.filter.BinaryComparator;

public class DecimalComparator extends BinaryComparator {

  protected byte[] msb = new byte[1];
  protected byte[] temp;

  /**
   * Constructor
   * @param value value
   */
  public DecimalComparator(byte[] value) {
    super(value);
    byte b = value[0];
    msb[0] = (byte) ((b >> 7) & 1);
    temp = new byte[value.length];
    System.arraycopy(value, 0, temp, 0, value.length);
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return super.compareTo(value, offset, length);
  }

}
