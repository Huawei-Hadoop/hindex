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
package org.apache.hadoop.hbase.index.coprocessor.regionserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.util.Pair;

public class IndexFilterNode implements LeafFilterNode {

  private IndexSpecification indexToUse;
  // all possible indices which can be used. This includes the selected indexToUse also.
  // This contains the an integer as the second item in the Pair. This is the relative overhead
  // in scanning the index region. The lesser the value the lesser the overhead in scanning the
  // index region. This will be set with the number of columns in the index specification.
  private List<Pair<IndexSpecification, Integer>> possibleUseIndices;

  private List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices;

  private FilterColumnValueDetail filterColumnValueDetail;

  @Override
  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
    // TODO avoid create of Map instance all the time...
    Map<Column, List<Pair<IndexSpecification, Integer>>> reply =
        new HashMap<Column, List<Pair<IndexSpecification, Integer>>>();
    reply.put(filterColumnValueDetail.getColumn(), possibleFutureUseIndices);
    return reply;
  }

  public IndexFilterNode(IndexSpecification indexToUse,
      List<Pair<IndexSpecification, Integer>> possibleUseIndices,
      List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices,
      FilterColumnValueDetail filterColumnValueDetail) {
    this.indexToUse = indexToUse;
    this.possibleUseIndices = possibleUseIndices;
    this.possibleFutureUseIndices = possibleFutureUseIndices;
    this.filterColumnValueDetail = filterColumnValueDetail;
  }

  /**
   * all possible indices which can be used. This includes the selected indexToUse also. This
   * contains the an integer as the second item in the Pair. This is the relative overhead in
   * scanning the index region. The lesser the value the lesser the overhead in scanning the index
   * region. This will be set with the number of columns in the index specification.
   * @return all possible indices can be used.
   */
  @Override
  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
    // TODO avoid create of Map instance all the time...
    Map<Column, List<Pair<IndexSpecification, Integer>>> reply =
        new HashMap<Column, List<Pair<IndexSpecification, Integer>>>();
    reply.put(filterColumnValueDetail.getColumn(), possibleUseIndices);
    return reply;
  }

  @Override
  public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
    // TODO avoid create of Map instance all the time...
    Map<List<FilterColumnValueDetail>, IndexSpecification> reply =
        new HashMap<List<FilterColumnValueDetail>, IndexSpecification>();
    List<FilterColumnValueDetail> key = new ArrayList<FilterColumnValueDetail>(1);
    key.add(filterColumnValueDetail);
    reply.put(key, indexToUse);
    return reply;
  }

  @Override
  public IndexSpecification getBestIndex() {
    return this.indexToUse;
  }

  @Override
  public FilterColumnValueDetail getFilterColumnValueDetail() {
    return this.filterColumnValueDetail;
  }

  public void setFilterColumnValueDetail(FilterColumnValueDetail filterColumnValueDetail) {
    this.filterColumnValueDetail = filterColumnValueDetail;
  }
}