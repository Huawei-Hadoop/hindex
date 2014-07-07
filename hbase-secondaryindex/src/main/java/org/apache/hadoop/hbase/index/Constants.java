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

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants contains all constants used in indexing
 *
 */
public class Constants {

  public static final int DEFAULT_NUM_RETRIES = 10;

  public static final long DEFAULT_PAUSE = 1000;

  public static final int DEFAULT_RETRY_LONGER_MULTIPLIER = 10;

  public static final byte[] IDX_COL_FAMILY = Bytes.toBytes("d");

  public static final byte[] IDX_COL_QUAL = new byte[0];

  public static final String INDEX_TABLE_SUFFIX = "_idx";

  public static final int DEF_MAX_INDEX_NAME_LENGTH = 18;

  public static final String INDEX_SPEC = "INDEX_SPEC";

  public static final String INDEX_BALANCER_DELEGATOR_CLASS =
      "hbase.index.loadbalancer.delegator.class";

  /**
   * Use this as a key to specify index details in {@link HTableDescriptor}
   * @see HTableDescriptor#setValue(byte[], byte[])
   */
  public static final byte[] INDEX_SPEC_KEY = Bytes.toBytes(INDEX_SPEC);

  /**
   * While scan the index(s) to be used can be explicitly passed from client application. Use this
   * as the name to pass it in attributes
   * @see Scan#setAttribute(String, byte[])
   */
  public static final String INDEX_EXPRESSION = "indexExpression";

}
