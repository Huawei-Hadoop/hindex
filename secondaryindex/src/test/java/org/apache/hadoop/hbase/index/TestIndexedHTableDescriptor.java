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
package org.apache.hadoop.hbase.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test whether index specification details valid or not
 */
@Category(MediumTests.class)
public class TestIndexedHTableDescriptor {

  @Test
  public void testAddIndex() throws Exception {
    IndexedHTableDescriptor iHtd = new IndexedHTableDescriptor("testAddIndex");
    List<IndexSpecification> indices = null;
    assertEquals("Table name is not equal to actual value.", "testAddIndex", iHtd.getNameAsString());
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    iHtd.addIndex(iSpec);
    indices = iHtd.getIndices();
    assertEquals("Index name should be equal with actual value.", "index_name", indices.get(0)
        .getName());
    assertTrue(
      "Column qualifier state mismatch.",
      indices.get(0).getIndexColumns()
          .contains(new ColumnQualifier("cf", "cq", ValueType.String, 10)));

  }

  @Test
  public void testAddIndexWithDuplicaIndexNames() throws Exception {
    IndexedHTableDescriptor iHtd = new IndexedHTableDescriptor("testAddIndexWithDuplicaIndexNames");
    assertEquals("Table name is not equal to actual value.", "testAddIndexWithDuplicaIndexNames",
      iHtd.getNameAsString());
    IndexSpecification iSpec = null;
    try {
      iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      iHtd.addIndex(iSpec);
      iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      iHtd.addIndex(iSpec);
      fail("Duplicate index names should not present for same table.");
    } catch (IllegalArgumentException e) {

    }

  }

  @Test
  public void testAddIndexWithIndexNameLengthGreaterThanMaxLength() throws Exception {
    IndexedHTableDescriptor iHtd =
        new IndexedHTableDescriptor("testAddIndexWithIndexNameLengthGreaterThanMaxLength");
    assertEquals("Table name is not equal to actual value.",
      "testAddIndexWithIndexNameLengthGreaterThanMaxLength", iHtd.getNameAsString());

    IndexSpecification iSpec = null;
    try {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 7; i++) {
        sb.append("index_name");
      }
      iSpec = new IndexSpecification(new String(sb));
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      iHtd.addIndex(iSpec);
      fail("Index name length should not be more than maximum length "
          + Constants.DEF_MAX_INDEX_NAME_LENGTH + '.');
    } catch (IllegalArgumentException e) {

    }

  }

  @Test
  public void testIndexedHTableDescriptorSerialization() throws Exception {
    ByteArrayOutputStream bos = null;
    DataOutputStream dos = null;
    ByteArrayInputStream bis = null;
    DataInputStream dis = null;
    try {
      bos = new ByteArrayOutputStream();
      dos = new DataOutputStream(bos);
      IndexedHTableDescriptor iHtd =
          new IndexedHTableDescriptor("testIndexedHTableDescriptorSerialization");
      HColumnDescriptor hcd = new HColumnDescriptor("cf");
      iHtd.addFamily(hcd);
      HColumnDescriptor hcd1 = new HColumnDescriptor("cf1");
      iHtd.addFamily(hcd1);
      IndexSpecification iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(hcd, "cq", ValueType.String, 10);
      iHtd.addIndex(iSpec);
      IndexSpecification iSpec1 = new IndexSpecification(Bytes.toBytes("index_name1"));
      iSpec1.addIndexColumn(hcd1, "cq1", ValueType.String, 10);
      iHtd.addIndex(iSpec1);
      iHtd.write(dos);
      dos.flush();
      byte[] byteArray = bos.toByteArray();
      bis = new ByteArrayInputStream(byteArray);
      dis = new DataInputStream(bis);
      IndexedHTableDescriptor iHtdRead = new IndexedHTableDescriptor();
      iHtdRead.readFields(dis);
      List<IndexSpecification> indices = iHtdRead.getIndices();

      IndexSpecification indexSpecification = indices.get(0);
      IndexSpecification indexSpecification1 = indices.get(1);
      assertEquals("Table name should match with actual value.", iHtdRead.getNameAsString(),
        "testIndexedHTableDescriptorSerialization");
      assertTrue("HColumnDescriptor state mismatch.",
        hcd.equals(iHtdRead.getFamily(Bytes.toBytes("cf"))));
      assertEquals("Index name mismatch.", indexSpecification.getName(), iSpec.getName());
      assertTrue(
        "ColumnQualifier state mismatch.",
        indexSpecification.getIndexColumns().contains(
          new ColumnQualifier("cf", "cq", ValueType.String, 10)));
      assertTrue(
        "ColumnQualifier state mismatch.",
        indexSpecification1.getIndexColumns().contains(
          new ColumnQualifier("cf1", "cq1", ValueType.String, 10)));
      // add finally to close
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

  @Test
  public void testAddIndexWithBlankIndexName() throws Exception {
    IndexedHTableDescriptor iHtd = new IndexedHTableDescriptor("testAddIndexWithBlankIndexName");
    assertEquals("Table name is not equal to actual value ", "testAddIndexWithBlankIndexName",
      iHtd.getNameAsString());

    IndexSpecification iSpec = null;
    try {
      iSpec = new IndexSpecification("  ");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 10);
      iHtd.addIndex(iSpec);
      fail("Index name should not be blank in Index Specification");
    } catch (IllegalArgumentException e) {

    }

  }

}
