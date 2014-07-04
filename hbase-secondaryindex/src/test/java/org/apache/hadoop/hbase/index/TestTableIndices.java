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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test whether index specification details valid or not
 */
@Category(MediumTests.class)
public class TestTableIndices {

  @Test
  public void testAddIndex() throws Exception {
    TableIndices indices = new TableIndices();
    IndexSpecification iSpec = new IndexSpecification("index_name");
    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
    indices.addIndex(iSpec);
    assertEquals("Index name should be equal with actual value.", "index_name", indices.getIndices().get(0)
        .getName());
    assertTrue(
      "Column qualifier state mismatch.",
      indices.getIndices().get(0).getIndexColumns()
          .contains(new ColumnQualifier("cf", "cq", ValueType.String, 10)));

  }

  @Test
  public void testAddIndexWithDuplicaIndexNames() throws Exception {
    TableIndices indices = new TableIndices();
    IndexSpecification iSpec = null;
    try {
      iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      indices.addIndex(iSpec);
      iSpec = new IndexSpecification("index_name");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      indices.addIndex(iSpec);
      fail("Duplicate index names should not present for same table.");
    } catch (IllegalArgumentException e) {

    }

  }

  @Test
  public void testAddIndexWithIndexNameLengthGreaterThanMaxLength() throws Exception {
    TableIndices indices = new TableIndices();
    IndexSpecification iSpec = null;
    try {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 7; i++) {
        sb.append("index_name");
      }
      iSpec = new IndexSpecification(new String(sb));
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", ValueType.String, 10);
      indices.addIndex(iSpec);
      fail("Index name length should not be more than maximum length "
          + Constants.DEF_MAX_INDEX_NAME_LENGTH + '.');
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testAddIndexWithBlankIndexName() throws Exception {
    TableIndices indices = new TableIndices();

    IndexSpecification iSpec = null;
    try {
      iSpec = new IndexSpecification("  ");
      iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 10);
      indices.addIndex(iSpec);
      fail("Index name should not be blank in Index Specification");
    } catch (IllegalArgumentException e) {

    }

  }

}
