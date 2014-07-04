/**
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
package org.apache.hadoop.hbase.index.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.junit.experimental.categories.Category;

/**
 * Test index manager functionality.
 */

@Category(MediumTests.class)
public class TestIndexManager extends TestCase {
  public void testAddIndexForTable() throws Exception {

    IndexManager im = IndexManager.getInstance();
    assertNotNull("Index Manager should not be null.", im);

    List<IndexSpecification> indexList = new ArrayList<IndexSpecification>(1);
    IndexSpecification iSpec = new IndexSpecification("index_name");

    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 10);
    indexList.add(iSpec);
    im.addIndexForTable("index_name", indexList);
    indexList = im.getIndicesForTable("index_name");
    assertEquals("Index name should be equal with actual value.", "index_name", indexList.get(0)
        .getName());
    assertTrue("Column qualifier state mismatch.",
      indexList.get(0).getIndexColumns().contains(new ColumnQualifier("cf", "cq", null, 10)));

  }

  public void testShouldNotThrowNPEIfValueTypeIsNull() throws Exception {
    IndexManager im = IndexManager.getInstance();
    assertNotNull("Index Manager should not be null.", im);

    List<IndexSpecification> indexList = new ArrayList<IndexSpecification>(1);
    IndexSpecification iSpec = new IndexSpecification("index_name");

    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 5);
    indexList.add(iSpec);
    im.addIndexForTable("index_name", indexList);
    indexList = im.getIndicesForTable("index_name");

    Set<ColumnQualifier> indexColumns = indexList.get(0).getIndexColumns();
    for (ColumnQualifier columnQualifier : indexColumns) {
      assertNotNull(columnQualifier.getType());
    }
  }

  public void testAddIndexForTableWhenStringAndValLengthIsZero() throws Exception {
    IndexManager im = IndexManager.getInstance();
    assertNotNull("Index Manager should not be null.", im);

    List<IndexSpecification> indexList = new ArrayList<IndexSpecification>(1);
    IndexSpecification iSpec = new IndexSpecification("index_name");

    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 0);
    indexList.add(iSpec);
    im.addIndexForTable("index_name", indexList);
    indexList = im.getIndicesForTable("index_name");
    assertEquals("the total value length should be 2", 2, indexList.get(0).getTotalValueLength());
  }

  public void testRemoveIndicesForTable() throws Exception {

    IndexManager im = IndexManager.getInstance();
    assertNotNull("Index Manager should not be null.", im);

    List<IndexSpecification> indexList = new ArrayList<IndexSpecification>(1);
    IndexSpecification iSpec = new IndexSpecification("index_name");

    iSpec.addIndexColumn(new HColumnDescriptor("cf"), "cq", null, 10);
    indexList.add(iSpec);
    im.removeIndices("index_name");
    indexList = im.getIndicesForTable("index_name");
    assertNull("Index specification List should be null.", indexList);

  }

}
