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
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.TableIndices;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class IndexCreationMapper extends TableMapper<ImmutableBytesWritable, Mutation> {

  private List<IndexSpecification> indices = new ArrayList<IndexSpecification>();
  private byte[][] startKeys = null;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    String tableNameToIndex = context.getConfiguration().get(TableIndexer.TABLE_NAME_TO_INDEX);
    HTable hTable = null;
    try {
      hTable = new HTable(context.getConfiguration(), tableNameToIndex);
      this.startKeys = hTable.getStartKeys();
      byte[] indexBytes = hTable.getTableDescriptor().getValue(Constants.INDEX_SPEC_KEY);
      if (indexBytes != null) {
        TableIndices tableIndices = new TableIndices();
        tableIndices.readFields(indexBytes);
        this.indices = tableIndices.getIndices();
      }
    } finally {
      if (hTable != null) hTable.close();
    }
  };

  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    resultToPut(context, row, value, conf);
  }

  private void resultToPut(Context context, ImmutableBytesWritable key, Result result,
      Configuration conf) throws IOException, InterruptedException {
    byte[] row = key.get();
    Put put = null;
    for (Cell kv : result.raw()) {
      if (((KeyValue) kv).isDelete()) {
        // Skipping delete records as any way the deletes will mask the actual
        // puts
        continue;
      } else {
        if (put == null) {
          put = new Put(row);
        }
        put.add(kv);
      }
    }
    if (put != null) {
      List<Put> indexPuts = IndexMapReduceUtil.getIndexPut(put, this.indices, this.startKeys, conf);
      for (Put idxPut : indexPuts) {
        context.write(new ImmutableBytesWritable(idxPut.getRow()), idxPut);
      }
    }
  }
}
