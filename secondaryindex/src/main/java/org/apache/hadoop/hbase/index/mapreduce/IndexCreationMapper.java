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
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class IndexCreationMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    resultToPut(context, row, value, conf);
  }

  private void resultToPut(Context context, ImmutableBytesWritable key, Result result,
      Configuration conf) throws IOException, InterruptedException {
    byte[] row = key.get();
    Put put = null;
    for (KeyValue kv : result.raw()) {
      if (kv.isDelete()) {
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
      List<Put> indexPuts = IndexMapReduceUtil.getIndexPut(put, conf);
      for (Put idxPut : indexPuts) {
        context.write(new ImmutableBytesWritable(idxPut.getRow()), idxPut);
      }
    }
  }
}
