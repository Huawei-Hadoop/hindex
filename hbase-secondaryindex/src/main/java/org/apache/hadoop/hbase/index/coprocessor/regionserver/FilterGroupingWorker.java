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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.index.filter.SingleColumnRangeFilter;
import org.apache.hadoop.hbase.index.filter.SingleColumnValuePartitionFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This class does the grouping work for different filterlists.
 * Like if we i have 3 different filter list and all have MUST_PASS_ALL condition
 * finally this class groups the filters lists into one filter list with all as MUST_PASS_ALL.
 * Also it checks if the combination of the filters list is given correctly.
 * For eg: If i have c1 > 10 and c1 < 5.  This is wrong combination.
 */
public class FilterGroupingWorker {

  private static final Log LOG = LogFactory.getLog(FilterGroupingWorker.class);

  private Map<Column, Pair<Value, Value>> colWithOperators =
      new HashMap<Column, Pair<Value, Value>>();
  private Map<Column, List<Value>> colWithOperatorsOfOR = new HashMap<Column, List<Value>>();

  public Filter group(Filter filter) {
    if (filter instanceof FilterList) {
      FilterList fList = (FilterList) filter;
      // We need to create a new FL here taking up only the filters of our interest
      FilterList newFList = new FilterList(fList.getOperator());
      List<Filter> filters = fList.getFilters();
      if (fList.getOperator() == Operator.MUST_PASS_ONE) {
        for (Filter subFilter : filters) {
          Filter resultFilter = handleFilterWithinOR(subFilter);
          // If result filter is not SingleColumnValueFilter or filter list that means OR branch is
          // having different type of filter other than SCVF. In that case we should not consider
          // the OR branch for scanning.
          if (resultFilter instanceof FilterList) {
            newFList.addFilter(resultFilter);
          } else if (resultFilter != null) {
            // This means OR filter list have at least one filter other than SCVF(may be other
            // child OR branches).
            return null;
          }
        }
        addORColsToFinalList(newFList);
        if (newFList.getFilters().isEmpty()) {
          return null;
        }
        return newFList;
      } else {
        // AND condition as long as the condition is AND in one sub tree all those can be
        // grouped under one AND parent(new one).
        for (Filter subFilter : filters) {
          Filter group = handleFilterWithinAND(subFilter);
          // group is null means, all are AND conditions and will be handled at once with the
          // below createFinalFilter
          if (group != null) {
            newFList.addFilter(group);
          }
        }
        addANDColsToFinalList(newFList);
        if (newFList.getFilters().isEmpty()) {
          return null;
        }
        return newFList;
      }
    } else if (filter instanceof SingleColumnValueFilter
        || filter instanceof SingleColumnRangeFilter) {
      return filter;
    }
    return null;
  }

  private Filter handleFilterWithinAND(Filter filter) {
    if (filter instanceof FilterList) {
      FilterList fList = (FilterList) filter;
      if (fList.getOperator() == Operator.MUST_PASS_ONE) {
        return new FilterGroupingWorker().group(fList);
      } else {
        List<Filter> filters = fList.getFilters();
        for (Filter subFilter : filters) {
          handleFilterWithinAND(subFilter);
        }
      }
    } else if (filter instanceof SingleColumnValueFilter) {
      handleScvf((SingleColumnValueFilter) filter);
    } // TODO when we expose SingleColumnRangeFilter to handle that also here.
    return null;
  }

  /**
   * Since you can use Filter Lists as children of Filter Lists, you can create a hierarchy of
   * filters to be evaluated. In the hierarchy if OR branch having any filter type other than SCVF
   * as child then we should not consider the branch for scanning because we cannot fetch seek
   * points from other type of filters without column and value details. Ex: AND AND
   * __________|_______ | | | --> SCVF OR SCVF _______|______ | | ROWFILTER SVCF If the OR is root
   * then we should skip index table scanning for this filter. OR _______|______ --> null | |
   * ROWFILTER SVCF If the OR is child of another OR branch then parent OR branch will be excluded
   * for scanning. Ex: AND AND __________|_______ | | | --> SCVF OR SCVF _______|______ | | OR SVCF
   * _______|______ | | ROWFILTER SVCF
   * @param filter
   * @return if filter is filter list with AND condition then we will return AND branch after
   *         grouping. if filter is filter list with OR condition return null if no children is of
   *         type other than SCVF or filter list else return different filter. if filter is SCVF
   *         then return null. returning null means we are combining the filter(s) with children of
   *         parent OR filter to perform optimizations.
   */
  private Filter handleFilterWithinOR(Filter filter) {
    if (filter instanceof FilterList) {
      FilterList fList = (FilterList) filter;
      if (fList.getOperator() == Operator.MUST_PASS_ONE) {
        List<Filter> filters = fList.getFilters();
        Filter resultFilter = null;
        for (Filter subFilter : filters) {
          // If this OR branch in the filter list have filter type other than SCVF we should report
          // it to parent by returning the other type of filter in such a way that the branch will
          // be skipped from index scan.
          resultFilter = handleFilterWithinOR(subFilter);
          if (resultFilter == null || (resultFilter instanceof FilterList)) {
            continue;
          } else {
            return resultFilter;
          }
        }
        return null;
      } else {
        return new FilterGroupingWorker().group(fList);
      }
    } else if (filter instanceof SingleColumnValueFilter) {
      handleScvfOfOR((SingleColumnValueFilter) filter);
      return null;
    }// TODO when we expose SingleColumnRangeFilter to handle that also here.
     // filter other than SingleColumnValueFilter.
    return filter;
  }

  private void handleScvfOfOR(SingleColumnValueFilter scvf) {
    ValuePartition vp = null;
    if (scvf instanceof SingleColumnValuePartitionFilter) {
      vp = ((SingleColumnValuePartitionFilter) scvf).getValuePartition();
    }
    Column key = new Column(scvf.getFamily(), scvf.getQualifier(), vp);
    if (colWithOperatorsOfOR.get(key) == null) {
      List<Value> valueList = new ArrayList<FilterGroupingWorker.Value>(1);
      valueList.add(new Value(scvf.getOperator(), scvf.getComparator().getValue(), scvf));
      colWithOperatorsOfOR.put(key, valueList);
    } else {
      List<Value> valueList = colWithOperatorsOfOR.get(key);
      Iterator<Value> valueListItr = valueList.iterator();
      CompareOp operator = scvf.getOperator();
      byte[] value = scvf.getComparator().getValue();
      Value prevValueObj = null;
      while (valueListItr.hasNext()) {
        prevValueObj = valueListItr.next();
        // TODO As Anoop said we may have to check the Value type also..
        // We can not compare and validate this way. btw "a" and "K".
        // Only in case of Numeric col type we can have this check.
        byte[] prevValue = prevValueObj.getValue();
        int result = Bytes.compareTo(prevValue, value);
        CompareOp prevOperator = prevValueObj.getOperator();
        switch (operator) {
        case GREATER:
          if (prevOperator == CompareOp.GREATER || prevOperator == CompareOp.GREATER_OR_EQUAL) {
            if (result > 0) {
              valueListItr.remove();
            } else {
              // Already you found less or equal value than present value means present filter will
              // return subset of previous filter. No need to add it to list.
              return;
            }
          } else if (prevOperator == CompareOp.LESS || prevOperator == CompareOp.LESS_OR_EQUAL) {
            // Need to handle conditions like previous is c1<5 and current c1>2. In these cases we
            // can change this condition into three parts c1<2,c1>=2 AND C1=<5 ,c1>5 and add to
            // list.
          } else if (prevOperator == CompareOp.EQUAL) {
            if (result > 0) {
              valueListItr.remove();
            } else if (result == 0) {
              // remove this entry and convert GREATER to GREATER_OR_EQUAL
              valueListItr.remove();
              SingleColumnValueFilter newScvf = null;
              if (vp == null) {
                newScvf =
                    new SingleColumnValueFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.GREATER_OR_EQUAL, scvf.getComparator());
              } else {
                newScvf =
                    new SingleColumnValuePartitionFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.GREATER_OR_EQUAL, scvf.getComparator(), vp);
              }
              Value newValue = new Value(CompareOp.GREATER_OR_EQUAL, prevValue, newScvf);
              valueList.add(newValue);
              return;
            }
          }
          break;
        case GREATER_OR_EQUAL:
          if (prevOperator == CompareOp.GREATER || prevOperator == CompareOp.GREATER_OR_EQUAL) {
            if (result >= 0) {
              valueListItr.remove();
            } else {
              // Already you found less value than present value means present filter will
              // return subset of previous filter. No need to add it to list.
              return;
            }
          } else if (prevOperator == CompareOp.LESS || prevOperator == CompareOp.LESS_OR_EQUAL) {
            // Need to handle conditions like previous is c1<5 and current c1>2. In these cases we
            // can change this condition into three parts c1<2,c1>=2 AND C1=<5 ,c1>5 and add to
            // list.
          } else if (prevOperator == CompareOp.EQUAL) {
            if (result >= 0) {
              valueListItr.remove();
            }
          }
          break;
        case LESS:
          if (prevOperator == CompareOp.LESS || prevOperator == CompareOp.LESS_OR_EQUAL) {
            if (result < 0) {
              valueListItr.remove();
            } else {
              // Already you found less or equal value than present value means present filter will
              // return subset of previous filter. No need to add it to list.
              return;
            }
          } else if (prevOperator == CompareOp.GREATER
              || prevOperator == CompareOp.GREATER_OR_EQUAL) {
            // Need to handle conditions like previous is c1<5 and current c1>2. In these cases we
            // can change this condition into three parts c1<2,c1>=2 AND C1=<5 ,c1>5 and add to
            // list.
          } else if (prevOperator == CompareOp.EQUAL) {
            if (result < 0) {
              valueListItr.remove();
            } else if (result == 0) {
              // remove this entry and convert LESS to LESS_OR_EQUAL
              valueListItr.remove();
              SingleColumnValueFilter newScvf = null;
              if (vp == null) {
                newScvf =
                    new SingleColumnValueFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.LESS_OR_EQUAL, scvf.getComparator());
              } else {
                newScvf =
                    new SingleColumnValuePartitionFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.LESS_OR_EQUAL, scvf.getComparator(), vp);
              }
              Value newValue = new Value(CompareOp.LESS_OR_EQUAL, prevValue, newScvf);
              valueList.add(newValue);
              return;
            }
          }
          break;
        case LESS_OR_EQUAL:
          if (prevOperator == CompareOp.LESS || prevOperator == CompareOp.LESS_OR_EQUAL) {
            if (result <= 0) {
              valueListItr.remove();
            } else {
              // Already you found less or equal value than present value means present filter will
              // return subset of previous filter. No need to add it to list.
              return;
            }
          } else if (prevOperator == CompareOp.GREATER
              || prevOperator == CompareOp.GREATER_OR_EQUAL) {
            // Need to handle conditions like previous is c1<5 and current c1>2. In these cases we
            // can change this condition into three parts c1<2,c1>=2 AND C1=<5 ,c1>5 and add to
            // list.
          } else if (prevOperator == CompareOp.EQUAL) {
            // If we dont want to do conversion we can add into first if condition.
            if (result <= 0) {
              valueListItr.remove();
            } else if (result == 0) {
              // remove this entry and convert GREATER to GREATER_OR_EQUAL
            }
          }
          break;
        case EQUAL:
          if (prevOperator == CompareOp.GREATER) {
            if (result < 0) {
              return;
            } else if (result == 0) {
              valueListItr.remove();
              SingleColumnValueFilter newScvf = null;
              if (vp == null) {
                newScvf =
                    new SingleColumnValueFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.GREATER_OR_EQUAL, scvf.getComparator());
              } else {
                newScvf =
                    new SingleColumnValuePartitionFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.GREATER_OR_EQUAL, scvf.getComparator(), vp);
              }
              Value newValue = new Value(CompareOp.GREATER_OR_EQUAL, prevValue, newScvf);
              valueList.add(newValue);
              return;
            }
          } else if (prevOperator == CompareOp.GREATER_OR_EQUAL) {
            if (result <= 0) {
              return;
            }
          } else if (prevOperator == CompareOp.LESS) {
            if (result > 0) {
              return;
            } else if (result == 0) {
              valueListItr.remove();
              SingleColumnValueFilter newScvf = null;
              if (vp == null) {
                newScvf =
                    new SingleColumnValueFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.LESS_OR_EQUAL, scvf.getComparator());
              } else {
                newScvf =
                    new SingleColumnValuePartitionFilter(scvf.getFamily(), scvf.getQualifier(),
                        CompareOp.LESS_OR_EQUAL, scvf.getComparator(), vp);
              }
              Value newValue = new Value(CompareOp.LESS_OR_EQUAL, prevValue, newScvf);
              valueList.add(newValue);
              return;
            }
          } else if (prevOperator == CompareOp.LESS_OR_EQUAL) {
            if (result >= 0) {
              return;
            }
          } else if (prevOperator == CompareOp.EQUAL) {
            if (result == 0) {
              // Already same filter exists with same condiftion.
              return;
            }
          }
          break;
        case NOT_EQUAL:
        case NO_OP:
          // Need to check this
          break;
        }
      }
      valueList.add(new Value(scvf.getOperator(), scvf.getComparator().getValue(), scvf));
    }
  }

  private void handleScvf(SingleColumnValueFilter scvf) {
    ValuePartition vp = null;
    if (scvf instanceof SingleColumnValuePartitionFilter) {
      vp = ((SingleColumnValuePartitionFilter) scvf).getValuePartition();
    }
    Column column = new Column(scvf.getFamily(), scvf.getQualifier(), vp);
    Pair<Value, Value> pair = colWithOperators.get(column);
    if (pair == null) {
      pair = new Pair<Value, Value>();
      // The first operator should be set here
      pair.setFirst(new Value(scvf.getOperator(), scvf.getComparator().getValue(), scvf));
      colWithOperators.put(column, pair);
    } else {
      if (pair.getFirst() != null && pair.getSecond() == null) {
        // TODO As Anoop said we may have to check the Value type also..
        // We can not compare and validate this way. btw "a" and "K".
        // Only in case of Numeric col type we can have this check.
        byte[] curBoundValue = scvf.getComparator().getValue();
        byte[] prevBoundValue = pair.getFirst().getValue();
        int result = Bytes.compareTo(prevBoundValue, curBoundValue);
        CompareOp curBoundOperator = scvf.getOperator();
        CompareOp prevBoundOperator = pair.getFirst().getOperator();
        switch (curBoundOperator) {
        case GREATER:
        case GREATER_OR_EQUAL:
          if (prevBoundOperator == CompareOp.GREATER
              || prevBoundOperator == CompareOp.GREATER_OR_EQUAL) {
            LOG.warn("Wrong usage.  It should be < > || > <.  Cannot be > >");
            if (result > 1) {
              pair.setFirst(new Value(curBoundOperator, curBoundValue, scvf));
            }
            pair.setSecond(null);
          } else if (prevBoundOperator == CompareOp.LESS
              || prevBoundOperator == CompareOp.LESS_OR_EQUAL) {
            if (result < 1) {
              LOG.warn("Possible wrong usage as there cannot be a value < 10 and  > 20");
              pair.setFirst(null);
              pair.setSecond(null);
            } else {
              pair.setSecond(new Value(curBoundOperator, curBoundValue, scvf));
            }
          } else if (prevBoundOperator == CompareOp.EQUAL) {
            LOG.warn("Use the equal operator and ignore the current one");
            pair.setSecond(null);
          }
          break;
        case LESS:
        case LESS_OR_EQUAL:
          if (prevBoundOperator == CompareOp.LESS || prevBoundOperator == CompareOp.LESS_OR_EQUAL) {
            LOG.warn("Wrong usage.  It should be < > || > <.  Cannot be > >");
            if (result < 1) {
              pair.setFirst(new Value(curBoundOperator, curBoundValue, scvf));
            }
            pair.setSecond(null);
          } else if (prevBoundOperator == CompareOp.GREATER
              || prevBoundOperator == CompareOp.GREATER_OR_EQUAL) {
            if (result > 1) {
              LOG.warn("Possible wrong usage as there cannot be a value < 10 and  > 20");
              pair.setFirst(null);
              pair.setSecond(null);
            } else {
              pair.setSecond(new Value(curBoundOperator, curBoundValue, scvf));
            }
          } else if (prevBoundOperator == CompareOp.EQUAL) {
            LOG.warn("Use the EQUAL operator only  and ignore the current one.");
            pair.setSecond(null);
          }
          break;
        case EQUAL:
          // For equal condition give priority to equals only..
          // If the prevOperator is also == and the current is also ==
          // take the second one.(Currently)
          if (prevBoundOperator == CompareOp.LESS || prevBoundOperator == CompareOp.LESS_OR_EQUAL
              || prevBoundOperator == CompareOp.EQUAL || prevBoundOperator == CompareOp.GREATER
              || prevBoundOperator == CompareOp.GREATER_OR_EQUAL) {
            pair.setFirst(new Value(curBoundOperator, curBoundValue, scvf));
            pair.setSecond(null);
          }
          break;
        case NOT_EQUAL:
        case NO_OP:
          // Need to check this
          break;
        }
      } else {
        LOG.warn("Am getting an extra comparison coming for the same col family."
            + "I cannot have 3 conditions on the same column");
        pair.setFirst(null);
        pair.setSecond(null);
      }
    }
  }

  private void addANDColsToFinalList(FilterList filterList) {
    for (Entry<Column, Pair<Value, Value>> entry : colWithOperators.entrySet()) {
      Pair<Value, Value> value = entry.getValue();
      if (value.getFirst() != null && value.getSecond() != null) {
        // Here we are introducing a new Filter
        SingleColumnRangeFilter rangeFltr =
            new SingleColumnRangeFilter(entry.getKey().getFamily(), entry.getKey().getQualifier(),
                entry.getKey().getValuePartition(), value.getFirst().getValue(), value.getFirst()
                    .getOperator(), value.getSecond().getValue(), value.getSecond().getOperator());
        filterList.addFilter(rangeFltr);
      } else if (value.getFirst() != null) {
        if (value.getFirst().getOperator() == CompareOp.EQUAL) {
          filterList.addFilter(value.getFirst().getFilter());
        } else {
          SingleColumnRangeFilter rangeFltr =
              new SingleColumnRangeFilter(entry.getKey().getFamily(),
                  entry.getKey().getQualifier(), entry.getKey().getValuePartition(), value
                      .getFirst().getValue(), value.getFirst().getOperator(), null, null);
          filterList.addFilter(rangeFltr);
        }
      }
    }
  }

  private void addORColsToFinalList(FilterList filterList) {
    for (Entry<Column, List<Value>> entry : colWithOperatorsOfOR.entrySet()) {
      List<Value> valueList = entry.getValue();
      for (Value value : valueList) {
        if (value.getOperator() == CompareOp.EQUAL) {
          filterList.addFilter(value.getFilter());
        } else {
          SingleColumnRangeFilter rangeFltr =
              new SingleColumnRangeFilter(entry.getKey().getFamily(),
                  entry.getKey().getQualifier(), entry.getKey().getValuePartition(),
                  value.getValue(), value.getOperator(), null, null);
          filterList.addFilter(rangeFltr);
        }
      }
    }
  }

  private static class Value {
    private CompareOp operator;
    private byte[] value;
    private Filter filter;

    public Value(CompareOp operator, byte[] value, Filter filter) {
      this.operator = operator;
      this.value = value;
      this.filter = filter;
    }

    public CompareOp getOperator() {
      return this.operator;
    }

    public byte[] getValue() {
      return this.value;
    }

    public Filter getFilter() {
      return this.filter;
    }
  }
}