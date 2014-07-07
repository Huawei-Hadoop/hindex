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

// Custom ByteArrayBuilder class to avoid overhead.
public class ByteArrayBuilder {
  private short pos;
  private byte[] store;

  public ByteArrayBuilder(int size) {
    store = new byte[size];
    pos = 0;
  }

  public void put(byte[] src) {
    System.arraycopy(src, 0, store, pos, src.length);
    pos += src.length;
  }

  public void put(byte[] src, int offset, int length) {
    System.arraycopy(src, offset, store, pos, length);
    pos += length;
  }

  public static ByteArrayBuilder allocate(int size) {
    return new ByteArrayBuilder(size);
  }

  public short position() {
    return pos;
  }

  public void position(int newPosition) {
    pos = (short) newPosition;
  }

  // Be careful calling this method. This method exposes the underlying byte[]
  // Any changes to this returned object will result in changes in the builder.
  public byte[] array() {
    return store;
  }

  // This method creates a new byte[] and copy the bytes from the underlying byte[]
  // Any changes to the returned object will not affect the builder.
  public byte[] array(int offset, int length) {
    byte[] subArray = new byte[length];
    System.arraycopy(store, offset, subArray, 0, length);
    return subArray;
  }
}
