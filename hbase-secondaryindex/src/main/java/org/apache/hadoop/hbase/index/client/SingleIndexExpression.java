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
package org.apache.hadoop.hbase.index.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.Constants;

/**
 * This can specify one index details to be used. The index can be single column or multi column
 * index. This index can be used for equals or range condition on columns. For a multi column index,
 * there can be 0 or more equals condition associated with this index in the scan and at max one
 * range condition (on a column). For an index the columns are in an order. There can not be any
 * equals condition specified on columns coming after the range conditioned column as per the
 * columns order in the index specification.
 * 
 * @see Scan#setAttribute(String, byte[])
 * @see Constants#INDEX_EXPRESSION
 * @see IndexUtils#toBytes(IndexExpression)
 */
public class SingleIndexExpression implements IndexExpression {

  private static final long serialVersionUID = 893160134306193043L;

  private String indexName;

  private List<EqualsExpression> equalsExpressions = new ArrayList<EqualsExpression>();

  private RangeExpression rangeExpression;

  public SingleIndexExpression(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  /**
   * This is expected to be called in the order of columns specified in the Index. If index is on
   * columns cf1:c1, cf1:c2 and cf2:c3 when creating the SingleIndexExpression call this method in
   * the same order as of above
   * @param equalsExpression
   */
  public void addEqualsExpression(EqualsExpression equalsExpression) {
    this.equalsExpressions.add(equalsExpression);
  }

  public List<EqualsExpression> getEqualsExpressions() {
    return this.equalsExpressions;
  }

  public void setRangeExpression(RangeExpression rangeExpression) {
    this.rangeExpression = rangeExpression;
  }

  public RangeExpression getRangeExpression() {
    return this.rangeExpression;
  }
}
