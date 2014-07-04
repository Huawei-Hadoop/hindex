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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestIndexUtils {

  private static final byte[] FAMILY1 = Bytes.toBytes("cf1");
  private static final byte[] QUALIFIER1 = Bytes.toBytes("c1");

  @Test
  public void testConvertingSimpleIndexExpressionToByteArray() throws Exception {
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression("idx1");
    Column column = new Column(FAMILY1, QUALIFIER1);
    byte[] value = "1".getBytes();
    EqualsExpression equalsExpression = new EqualsExpression(column, value);
    singleIndexExpression.addEqualsExpression(equalsExpression);

    byte[] bytes = IndexUtils.toBytes(singleIndexExpression);
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bis);
    SingleIndexExpression readExp = (SingleIndexExpression) ois.readObject();
    assertEquals("idx1", readExp.getIndexName());
    assertEquals(1, readExp.getEqualsExpressions().size());
    assertTrue(Bytes.equals(value, readExp.getEqualsExpressions().get(0).getValue()));
    assertEquals(column, readExp.getEqualsExpressions().get(0).getColumn());
  }

  @Test
  public void testConvertingBytesIntoIndexExpression() throws Exception {
    SingleIndexExpression singleIndexExpression = new SingleIndexExpression("idx1");
    Column column = new Column(FAMILY1, QUALIFIER1);
    byte[] value = "1".getBytes();
    EqualsExpression equalsExpression = new EqualsExpression(column, value);
    singleIndexExpression.addEqualsExpression(equalsExpression);

  }
}
