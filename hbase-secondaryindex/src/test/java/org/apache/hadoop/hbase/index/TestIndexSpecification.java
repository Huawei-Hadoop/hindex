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
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test whether {@link ColumnQualifier} details provided for {@link IndexSpecification} are valid or
 * not.
 */
@Category(MediumTests.class)
public class TestIndexSpecification {

  @Test
  public void testInvalidIndexSpecName() throws Exception {
    // Contains =>
    try {
      new IndexSpecification("index=>name");
      fail("IllegalArgexception should be thrown.");
    } catch (IllegalArgumentException e) {
    }
    // Contains ,
    try {
      new IndexSpecification("index,name");
      fail("IllegalArgexception should be thrown.");
    } catch (IllegalArgumentException e) {
    }
    // Contains ?
    try {
      new IndexSpecification("index?name");
      fail("IllegalArgexception should be thrown.");
    } catch (IllegalArgumentException e) {
    }
    // Contains numbers
    try {
      new IndexSpecification("index0name");
    } catch (IllegalArgumentException e) {
      fail("Illegal Arg should not be thrown.");
    }
    // Contains numbers at beginning and end
    try {
      new IndexSpecification("0index0name0");
    } catch (IllegalArgumentException e) {
      fail("Illegal Arg should not be thrown.");
    }
    // Contains '.' at the beginning
    try {
      new IndexSpecification(".indexname");
      fail("IllegalArgexception should be thrown.");
    } catch (IllegalArgumentException e) {
    }
    // Contains "-" at the beginnning
    try {
      new IndexSpecification("-indexname");
      fail("IllegalArgexception should be thrown.");
    } catch (IllegalArgumentException e) {
    }
    // Contains '.' in the middle
    try {
      new IndexSpecification("index.name");
    } catch (IllegalArgumentException e) {
      fail("Illegal Arg should not be thrown.");
    }
    // Contains '-' in the middle
    try {
      new IndexSpecification("index-name");
    } catch (IllegalArgumentException e) {
      fail("Illegal Arg should not be thrown.");
    }
  }

  @Test
  public void testGetName() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    assertEquals("Index name not match with acutal index name.", "index_name", iSpec.getName());
    iSpec = new IndexSpecification("index_name");
    assertEquals("Index name not match with acutal index name.", "index_name", iSpec.getName());

  }

  @Test
  public void testAddIndexColumnWithNotNull() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    Set<ColumnQualifier> indexColumns = iSpec.getIndexColumns();
    assertEquals("Size of column qualifiers list should be zero.", 0, indexColumns.size());
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    indexColumns = iSpec.getIndexColumns();
    assertTrue("ColumnQualifier state mismatch.",
      indexColumns.contains(new ColumnQualifier("cf", "cq", ValueType.String, 10)));
  }

  @Test
  public void testAddIndexColumnWithNullCF() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(null, "cq", ValueType.String, 10);
      fail("Column family name should not be null in index specification.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithNullQualifier() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), null, ValueType.String, 10);
      fail("Column qualifier name should not be null in index specification.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithBlankCForQualifier() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("  "), "   ", ValueType.String, 10);
      fail("Column family name and qualifier should not be blank.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithBlankCF() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("  "), "cq", ValueType.String, 10);
      fail("Column family name should not be blank.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithBlankQualifier() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), " ", ValueType.String, 10);
      fail("Column qualifier name should not be blank.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithControlCharactersOrColonsInCF() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("cf:"), "cq", ValueType.String, 10);
      fail("Family names should not contain control characters or colons.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexColumnWithDuplicaCFandQualifier() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    try {
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      fail("Column familily and qualifier combination should not be"
          + " repeated in Index Specification.");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testMinTTLCalculation() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec
        .addIndexColumn(new HColumnDescriptor("cf").setTimeToLive(100), "cq", ValueType.String, 10);
    iSpec
        .addIndexColumn(new HColumnDescriptor("cf1").setTimeToLive(50), "cq", ValueType.String, 10);
    assertEquals("The ttl should be 50", 50, iSpec.getTTL());
  }

  @Test
  public void testMaxVersionsCalculation() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf").setMaxVersions(100), "cq", ValueType.String,
      10);
    iSpec.addIndexColumn(new HColumnDescriptor("cf1").setMaxVersions(50), "cq", ValueType.String,
      10);
    assertEquals("The max versions should be 50", 50, iSpec.getMaxVersions());
  }

  @Test
  public void testMinTTLWhenDefault() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    iSpec.addIndexColumn(new HColumnDescriptor("cf1"), "cq", ValueType.String, 10);
    assertEquals("The ttl should be " + Integer.MAX_VALUE, HConstants.FOREVER, iSpec.getTTL());
  }

  @Test
  public void testMinTTLWhenCombinationOfDefaultAndGivenValue() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    iSpec
        .addIndexColumn(new HColumnDescriptor("cf1").setTimeToLive(50), "cq", ValueType.String, 10);
    assertEquals("The ttl should be 50", 50, iSpec.getTTL());
  }

  @Test
  public void testMaxVersionsWhenDefault() throws Exception {
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    iSpec.addIndexColumn(new HColumnDescriptor("cf1"), "cq", ValueType.String, 10);
    assertEquals("default max versions should be 1", 1, iSpec.getMaxVersions());
  }

  @Test
  public void testIndexSpecificationSerialization() throws Exception {
    ByteArrayOutputStream bos = null;
    DataOutputStream dos = null;
    ByteArrayInputStream bis = null;
    DataInputStream dis = null;
    try {
      bos = new ByteArrayOutputStream();
      dos = new DataOutputStream(bos);
      IndexSpecification iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      iSpec.write(dos);
      dos.flush();
      byte[] byteArray = bos.toByteArray();
      bis = new ByteArrayInputStream(byteArray);
      dis = new DataInputStream(bis);
      IndexSpecification indexSpecification = new IndexSpecification();
      indexSpecification.readFields(dis);
      assertEquals("Index name mismatch.", indexSpecification.getName(), iSpec.getName());
      assertTrue("ColumnQualifier state mismatch.",
        iSpec.getIndexColumns().contains(new ColumnQualifier("cf", "cq", ValueType.String, 10)));
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
