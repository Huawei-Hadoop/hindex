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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Client side utility class for using Secondary Index.
 */
public class IndexUtils {

  private IndexUtils() {
    // Avoid instantiation of this class.
  }

  /**
   * Utility to convert IndexExpression into byte[]. Can be used to pass the IndexExpression in the
   * Scan attributes.
   * @see Scan#setAttribute(String, byte[])
   * @param indexExpression
   * @return byte[]
   * @throws IOException
   */
  public static byte[] toBytes(IndexExpression indexExpression) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(indexExpression);
    return bos.toByteArray();
  }

  /**
   * Creates back IndexExpression from byte[]
   * @param bytes
   * @return IndexExpression deserialized from the specified bytes.
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static IndexExpression toIndexExpression(byte[] bytes) throws IOException,
      ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bis);
    return (IndexExpression) ois.readObject();
  }
}
