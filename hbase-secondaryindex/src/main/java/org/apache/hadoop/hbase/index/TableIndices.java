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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;


/**
 * TableIndices contains indices to specify index name and column details. There can be one or more
 * indices on one table. For each of the index on the table and IndexSpecification is to be created
 * and added to the indices.
 */
public class TableIndices {

  private static final Log LOG = LogFactory.getLog(TableIndices.class);

  private List<IndexSpecification> indices = new ArrayList<IndexSpecification>(1);

  public TableIndices() {

  }

  /**
   * @param iSpec index specification to be added to indices
   * @throws IllegalArgumentException if duplicate indexes for same table
   */
  public void addIndex(IndexSpecification iSpec) throws IllegalArgumentException {
    String indexName = iSpec.getName();
    if (null == indexName) {
      String message = "Index name should not be null in Index Specification.";
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }
    if (true == StringUtils.isBlank(indexName)) {
      String message = "Index name should not be blank in Index Specification.";
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }
    if (indexName.length() > Constants.DEF_MAX_INDEX_NAME_LENGTH) {
      String message =
          "Index name length should not more than " + Constants.DEF_MAX_INDEX_NAME_LENGTH + '.';
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }
    for (IndexSpecification is : indices) {
      if (is.getName().equals(indexName)) {
        String message = "Duplicate index names should not be present for same table.";
        LOG.error(message);
        throw new IllegalArgumentException(message);
      }
    }
    indices.add(iSpec);
  }

  /**
   * @return IndexSpecification list
   */
  public List<IndexSpecification> getIndices() {
    return (new ArrayList<IndexSpecification>(this.indices));
  }

  /**
   * @param out Data output stream
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.indices.size());
    for (IndexSpecification index : indices) {
      index.write(out);
    }
  }

  /**
   * @param bytes
   * @throws IOException
   */
  public void readFields(byte[] bytes) throws IOException {
    ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
    int indicesSize = in.readInt();
    indices.clear();
    for (int i = 0; i < indicesSize; i++) {
      IndexSpecification is = new IndexSpecification();
      is.readFields(in);
      this.indices.add(is);
    }
  }

  public byte[] toByteArray() throws IOException {
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    write(out);
    return out.toByteArray();
  }
}
