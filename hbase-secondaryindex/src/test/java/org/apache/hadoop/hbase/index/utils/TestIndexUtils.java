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
package org.apache.hadoop.hbase.index.utils;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestIndexUtils {

  @Test
  public void testIncrementValue1() throws Exception {
    byte[] val = new byte[] { 97, 97, 97, 97, 127 };
    byte[] incrementValue = IndexUtils.incrementValue(val, true);
    assertEquals(Bytes.compareTo(incrementValue, val), 1);
  }

  @Test
  public void testIncrementValue2() throws Exception {
    byte[] val = new byte[] { 1, 127, 127, 127, 127, 127 };
    byte[] incrementValue = IndexUtils.incrementValue(val, true);
    assertEquals(Bytes.compareTo(incrementValue, val), 1);
  }

  @Test
  public void testIncrementValue3() throws Exception {
    byte[] val = new byte[] { 127, 127, 127, 127, -128 };
    byte[] incrementValue = IndexUtils.incrementValue(val, true);
    assertEquals(Bytes.compareTo(incrementValue, val), 1);
  }

  @Test
  public void testIncrementValue4() throws Exception {
    byte[] val = new byte[] { -1, -1, -1, -1, -1 };
    byte[] incrementValue = IndexUtils.incrementValue(val, true);
    assertEquals(Bytes.compareTo(incrementValue, new byte[] { 0, 0, 0, 0, 0 }), 0);
  }

  @Test
  public void testIncrementValue5() throws Exception {
    byte[] val = new byte[] { 56, 57, 58, -1, 127 };
    byte[] incrementValue = IndexUtils.incrementValue(val, true);
    assertEquals(Bytes.compareTo(incrementValue, new byte[] { 56, 57, 58, -1, -128 }), 0);
  }

}
