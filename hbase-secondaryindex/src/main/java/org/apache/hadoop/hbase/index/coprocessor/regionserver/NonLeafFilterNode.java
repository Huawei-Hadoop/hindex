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
import org.apache.hadoop.hbase.index.GroupingCondition;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.util.Pair;

public class NonLeafFilterNode implements FilterNode {

  private List<FilterNode> filterNodes = new ArrayList<FilterNode>();

  private GroupingCondition groupingCondition;

  private Map<List<FilterColumnValueDetail>, IndexSpecification> indicesToUse =
      new HashMap<List<FilterColumnValueDetail>, IndexSpecification>();

  public NonLeafFilterNode(GroupingCondition condition) {
    this.groupingCondition = condition;
  }

  public GroupingCondition getGroupingCondition() {
    return groupingCondition;
  }

  public List<FilterNode> getFilterNodes() {
    return filterNodes;
  }

  public void addFilterNode(FilterNode filterNode) {
    this.filterNodes.add(filterNode);
  }

  public void addIndicesToUse(FilterColumnValueDetail f, IndexSpecification i) {
    List<FilterColumnValueDetail> key = new ArrayList<FilterColumnValueDetail>(1);
    key.add(f);
    this.indicesToUse.put(key, i);
  }

  public void addIndicesToUse(List<FilterColumnValueDetail> lf, IndexSpecification i) {
    this.indicesToUse.put(lf, i);
  }

  @Override
  public Map<List<FilterColumnValueDetail>, IndexSpecification> getIndexToUse() {
    return this.indicesToUse;
  }

  @Override
  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleUseIndices() {
    return null;
  }

  @Override
  public Map<Column, List<Pair<IndexSpecification, Integer>>> getPossibleFutureUseIndices() {
    // There is no question of future use possible indices on a non leaf node.
    return null;
  }
}