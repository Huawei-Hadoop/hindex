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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test setting values in ColumnQuafilier.
 */
@Category(MediumTests.class)
public class TestColumnQualifier {

  @Test
  public void testColumnQualifier() throws Exception {
    ColumnQualifier cq = new ColumnQualifier("cf", "cq");
    assertEquals("Column family not match with acutal value.", "cf", cq.getColumnFamilyString());
    assertEquals("Column qualifier not match with actual value.", "cq", cq.getQualifierString());
    assertEquals("Column family bytes not match with actual value.", 0,
      Bytes.compareTo(Bytes.toBytes("cf"), cq.getColumnFamily()));
    assertEquals("Column qualifier bytes not match with actual value.", 0,
      Bytes.compareTo(Bytes.toBytes("cq"), cq.getQualifier()));
  }

  public void testColumQualifierEquals() {
    ColumnQualifier cq = new ColumnQualifier(Bytes.toBytes("cf"), Bytes.toBytes("cq"));
    assertTrue("ColumnQualifier state mismatch.",
      cq.equals(new ColumnQualifier(Bytes.toBytes("cf"), Bytes.toBytes("cq"))));
    assertTrue("ColumnQualifier state mismatch.", cq.equals(new ColumnQualifier("cf", "cq")));
  }

  @Test
  public void testColumnQualifierSerialization() throws Exception {
    ByteArrayOutputStream bos = null;
    DataOutputStream dos = null;
    ByteArrayInputStream bis = null;
    DataInputStream dis = null;
    try {
      bos = new ByteArrayOutputStream();
      dos = new DataOutputStream(bos);
      ColumnQualifier cq =
          new ColumnQualifier("cf", "cq", ValueType.String, 10, new SeparatorPartition("--", 10));
      cq.write(dos);
      dos.flush();
      byte[] byteArray = bos.toByteArray();
      bis = new ByteArrayInputStream(byteArray);
      dis = new DataInputStream(bis);
      ColumnQualifier c = new ColumnQualifier();
      c.readFields(dis);
      ValuePartition vp = c.getValuePartition();
      if (vp instanceof SeparatorPartition) {

      } else {
        fail("value partition details no read properly.");
      }
      assertTrue("ColumnQualifier state mismatch.", c.equals(cq));
    } finally {
      if (null != bos) {
        bos.close();
      }
      if (null != dos) {
        dos.close();
      }
      if (null != bis) {
        bis.close();
      }
      if (null != dis) {
        dis.close();
      }
    }

  }

}
