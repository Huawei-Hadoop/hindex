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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.util.Pair;

public class PossibleIndexFilterNode implements LeafFilterNode {

  private List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices;

  private FilterColumnValueDetail filterColumnValueDetail;

  public PossibleIndexFilterNode(List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices,
      FilterColumnValueDetail filterColumnValueDetail) {
    this.possibleFutureUseIndices = possibleFutureUseIndices;
    this.filterColumnValueDetail = filterColumnValueDetail;
  }

  @Override
  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
    // TODO avoid create of Map instance all the time...
    Map<Column, List<Pair<IndexSpecification, Integer>>> reply =
        new HashMap<Column, List<Pair<IndexSpecification, Integer>>>();
    reply.put(filterColumnValueDetail.getColumn(), possibleFutureUseIndices);
    return reply;
  }

  @Override
  public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
    return null;
  }

  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
    return null;
  }

  @Override
  public FilterColumnValueDetail getFilterColumnValueDetail() {
    return this.filterColumnValueDetail;
  }

  @Override
  public void setFilterColumnValueDetail(FilterColumnValueDetail filterColumnValueDetail) {
    this.filterColumnValueDetail = filterColumnValueDetail;
  }

  @Override
  public IndexSpecification getBestIndex() {
    return null;
  }
}