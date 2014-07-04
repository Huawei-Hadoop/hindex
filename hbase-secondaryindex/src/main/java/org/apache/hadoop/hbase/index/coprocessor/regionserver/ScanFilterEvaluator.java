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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.Column;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.GroupingCondition;
import org.apache.hadoop.hbase.index.ValuePartition;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.client.EqualsExpression;
import org.apache.hadoop.hbase.index.client.IndexExpression;
import org.apache.hadoop.hbase.index.client.MultiIndexExpression;
import org.apache.hadoop.hbase.index.client.NoIndexExpression;
import org.apache.hadoop.hbase.index.client.RangeExpression;
import org.apache.hadoop.hbase.index.client.SingleIndexExpression;
import org.apache.hadoop.hbase.index.filter.SingleColumnRangeFilter;
import org.apache.hadoop.hbase.index.filter.SingleColumnValuePartitionFilter;
import org.apache.hadoop.hbase.index.manager.IndexManager;
import org.apache.hadoop.hbase.index.util.ByteArrayBuilder;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class ScanFilterEvaluator {

  private static final Log LOG = LogFactory.getLog(ScanFilterEvaluator.class);

  private static final Set<IndexSpecification> EMPTY_INDEX_SET = new HashSet<IndexSpecification>(0);

  // TODO we can make this class singleton? This cache will be shared by all regions.
  // Then need to change the Map to include the table info also. A Map<Map>
  private Map<ColumnWithValue, FilterNode> colFilterNodeCache =
      new ConcurrentHashMap<ColumnWithValue, FilterNode>();

  /**
   * This method will evaluate the filter(s) used along with the user table scan against the indices
   * available on the table and will provide a list of scans we would like to perform on the index
   * table corresponding to this user scan.
   * @param scan
   * @param indices
   * @param regionStartKey
   * @param idxRegion
   * @param tableName
   * @return index region scanner
   * @throws IOException
   */
  public IndexRegionScanner evaluate(Scan scan, List<IndexSpecification> indices,
      byte[] regionStartKey, HRegion idxRegion, String tableName) throws IOException {
    Filter filter = scan.getFilter();
    byte[] indexExpBytes = scan.getAttribute(Constants.INDEX_EXPRESSION);
    if (filter == null) {
      // When the index(s) to be used is passed through scan attributes and Scan not having any
      // filters it is invalid
      if (indexExpBytes != null) {
        LOG.warn("Passed an Index expression along with the Scan but without any filters on Scan!"
            + " The index wont be used");
      }
      return null;
    }
    FilterNode node = null;
    IndexRegionScanner indexRegionScanner = null;
    if (indexExpBytes != null) {
      // Which index(s) to be used is already passed by the user.
      try {
        IndexExpression indexExpression =
            org.apache.hadoop.hbase.index.client.IndexUtils.toIndexExpression(indexExpBytes);
        if (indexExpression instanceof NoIndexExpression) {
          // Client side says not to use any index for this Scan.
          LOG.info("NoIndexExpression is passed as the index to be used for this Scan."
              + " No possible index will be used.");
          return null;
        }
        Map<String, IndexSpecification> nameVsIndex = new HashMap<String, IndexSpecification>();
        for (IndexSpecification index : indices) {
          nameVsIndex.put(index.getName(), index);
        }
        node = convertIdxExpToFilterNode(indexExpression, nameVsIndex, tableName);
      } catch (Exception e) {
        LOG.error("There is an Exception in getting IndexExpression from Scan attribute!"
            + " The index won't be used", e);
      }
    } else {
      Filter newFilter = doFiltersRestruct(filter);
      if (newFilter != null) {
        node = evalFilterForIndexSelection(newFilter, indices);
      }
    }
    if (node != null) {
      indexRegionScanner =
          createIndexScannerScheme(node, regionStartKey, scan.getStartRow(), scan.getStopRow(),
            idxRegion, tableName);
      // TODO - This check and set looks ugly. Correct me..
      if (indexRegionScanner instanceof IndexRegionScannerForOR) {
        ((IndexRegionScannerForOR) indexRegionScanner).setRootFlag(true);
      }
      if (indexRegionScanner != null) {
        indexRegionScanner.setScannerIndex(0);
        if (indexRegionScanner.hasChildScanners()) {
          return indexRegionScanner;
        } else {
          return null;
        }
      }
    }
    return indexRegionScanner;
  }

  private FilterNode convertIdxExpToFilterNode(IndexExpression indexExpression,
      Map<String, IndexSpecification> nameVsIndex, String tableName) {
    if (indexExpression instanceof MultiIndexExpression) {
      MultiIndexExpression mie = (MultiIndexExpression) indexExpression;
      NonLeafFilterNode nlfn = new NonLeafFilterNode(mie.getGroupingCondition());
      for (IndexExpression ie : mie.getIndexExpressions()) {
        FilterNode fn = convertIdxExpToFilterNode(ie, nameVsIndex, tableName);
        nlfn.addFilterNode(fn);
      }
      return nlfn;
    } else {
      // SingleIndexExpression
      SingleIndexExpression sie = (SingleIndexExpression) indexExpression;
      IndexSpecification index = nameVsIndex.get(sie.getIndexName());
      if (index == null) {
        throw new RuntimeException("No index:" + sie.getIndexName() + " added for table:"
            + tableName);
      }
      Map<Column, ColumnQualifier> colVsCQ = new HashMap<Column, ColumnQualifier>();
      for (ColumnQualifier cq : index.getIndexColumns()) {
        colVsCQ
            .put(new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition()), cq);
      }
      // TODO -- seems some thing wrong with IndexFilterNode! Ideally I need to create that here.
      NonLeafFilterNode nlfn = new NonLeafFilterNode(null);
      List<FilterColumnValueDetail> fcvds = new ArrayList<FilterColumnValueDetail>();
      // Here by we expect that the equals expressions are given the order of columns in index.
      // TODO add reordering if needed?
      for (EqualsExpression ee : sie.getEqualsExpressions()) {
        ColumnQualifier cq = colVsCQ.get(ee.getColumn());
        if (cq == null) {
          throw new RuntimeException("The column:[" + ee.getColumn() + "] is not a part of index: "
              + sie.getIndexName());
        }
        FilterColumnValueDetail fcvd =
            new FilterColumnValueDetail(ee.getColumn(), ee.getValue(), CompareOp.EQUAL);
        fcvd.maxValueLength = cq.getMaxValueLength();
        fcvd.valueType = cq.getType();
        fcvds.add(fcvd);
      }
      // RangeExpression to come after the EqualsExpressions
      RangeExpression re = sie.getRangeExpression();
      if (re != null) {
        ColumnQualifier cq = colVsCQ.get(re.getColumn());
        if (cq == null) {
          throw new RuntimeException("The column:[" + re.getColumn() + "] is not a part of index: "
              + sie.getIndexName());
        }
        CompareOp lowerBoundCompareOp =
            re.isLowerBoundInclusive() ? CompareOp.GREATER_OR_EQUAL : CompareOp.GREATER;
        CompareOp upperBoundCompareOp =
            re.isUpperBoundInclusive() ? CompareOp.LESS_OR_EQUAL : CompareOp.LESS;
        if (re.getLowerBoundValue() == null) {
          lowerBoundCompareOp = null;
        }
        if (re.getUpperBoundValue() == null) {
          upperBoundCompareOp = null;
        }
        FilterColumnValueRange fcvr =
            new FilterColumnValueRange(re.getColumn(), re.getLowerBoundValue(),
                lowerBoundCompareOp, re.getUpperBoundValue(), upperBoundCompareOp);
        fcvr.maxValueLength = cq.getMaxValueLength();
        fcvr.valueType = cq.getType();
        fcvds.add(fcvr);
      }
      nlfn.addIndicesToUse(fcvds, index);
      return nlfn;
    }
  }

  IndexRegionScanner createIndexScannerScheme(FilterNode node, byte[] regionStartKey,
      byte[] startRow, byte[] stopRow, HRegion indexRegion, String userTableName)
      throws IOException {
    List<IndexRegionScanner> scanners = new ArrayList<IndexRegionScanner>();
    IndexRegionScanner idxScanner = null;
    int scannerIndex = -1;
    if (node instanceof NonLeafFilterNode) {
      boolean hasRangeScanner = false;
      NonLeafFilterNode nlfNode = (NonLeafFilterNode) node;
      Map<List<FilterColumnValueDetail>, IndexSpecification> indicesToUse = nlfNode.getIndexToUse();
      for (Entry<List<FilterColumnValueDetail>, IndexSpecification> entry : indicesToUse.entrySet()) {
        List<FilterColumnValueDetail> fcvdList = entry.getKey();
        byte[] indexName = Bytes.toBytes(entry.getValue().getName());
        ByteArrayBuilder indexNameBuilder =
            ByteArrayBuilder.allocate(IndexUtils.getMaxIndexNameLength());
        indexNameBuilder.put(indexName);
        Scan scan = createScan(regionStartKey, indexName, fcvdList, startRow, stopRow);
        boolean isRange = isHavingRangeFilters(fcvdList);
        if (!hasRangeScanner && isRange) hasRangeScanner = isRange;
        createRegionScanner(indexRegion, userTableName, scanners, indexNameBuilder, scan, isRange,
          ++scannerIndex);
      }
      for (FilterNode fn : nlfNode.getFilterNodes()) {
        IndexRegionScanner childIndexScaner =
            createIndexScannerScheme(fn, regionStartKey, startRow, stopRow, indexRegion,
              userTableName);
        childIndexScaner.setScannerIndex(++scannerIndex);
        scanners.add(childIndexScaner);
        if (!hasRangeScanner) hasRangeScanner = childIndexScaner.isRange();
      }
      idxScanner = createScannerForNonLeafNode(scanners, nlfNode.getGroupingCondition());
      idxScanner.setRangeFlag(hasRangeScanner);
      return idxScanner;
    } else if (node instanceof PossibleIndexFilterNode) {
      LOG.info("No index can be used for the column "
          + ((PossibleIndexFilterNode) node).getFilterColumnValueDetail().getColumn());
      return null;
    } else if (node instanceof IndexFilterNode) {
      // node is IndexFilterNode
      IndexFilterNode ifNode = (IndexFilterNode) node;
      // There will be only one entry in this Map
      List<FilterColumnValueDetail> filterColsDetails =
          ifNode.getIndexToUse().keySet().iterator().next();
      byte[] indexName = Bytes.toBytes(ifNode.getBestIndex().getName());
      ByteArrayBuilder indexNameBuilder =
          ByteArrayBuilder.allocate(IndexUtils.getMaxIndexNameLength());
      indexNameBuilder.put(indexName);
      Scan scan = createScan(regionStartKey, indexName, filterColsDetails, startRow, stopRow);
      boolean isRange = isHavingRangeFilters(filterColsDetails);
      createRegionScanner(indexRegion, userTableName, scanners, indexNameBuilder, scan, isRange,
        ++scannerIndex);
      idxScanner = createScannerForNonLeafNode(scanners, null);
      idxScanner.setRangeFlag(isRange);
      return idxScanner;
    }
    return null;
  }

  private boolean isHavingRangeFilters(List<FilterColumnValueDetail> fcvdList) {
    for (FilterColumnValueDetail fcvd : fcvdList) {
      if (fcvd instanceof FilterColumnValueRange) {
        return true;
      }
    }
    return false;
  }

  private IndexRegionScanner createScannerForNonLeafNode(List<IndexRegionScanner> scanners,
      GroupingCondition condition) {
    IndexRegionScanner idxScanner;
    if (condition == GroupingCondition.OR) {
      idxScanner = new IndexRegionScannerForOR(scanners);
    } else {
      idxScanner = new IndexRegionScannerForAND(scanners);
    }
    return idxScanner;
  }

  private void createRegionScanner(HRegion indexRegion, String userTableName,
      List<IndexRegionScanner> scanners, ByteArrayBuilder indexNameBuilder, Scan scan,
      boolean isRange, int scannerIndex) throws IOException {
    RegionScanner scannerForIndexRegion = indexRegion.getScanner(scan);
    LeafIndexRegionScanner leafIndexRegionScanner =
        new LeafIndexRegionScanner(IndexManager.getInstance().getIndex(userTableName,
          indexNameBuilder.array()), scannerForIndexRegion, new TTLExpiryChecker());
    leafIndexRegionScanner.setScannerIndex(scannerIndex);
    leafIndexRegionScanner.setRangeFlag(isRange);
    scanners.add(leafIndexRegionScanner);
  }

  private Scan createScan(byte[] regionStartKey, byte[] indexName,
      List<FilterColumnValueDetail> filterColsDetails, byte[] startRow, byte[] stopRow) {
    Scan scan = new Scan();
    // common key is the regionStartKey + indexName
    byte[] commonKey = createCommonKeyForIndex(regionStartKey, indexName);
    scan.setStartRow(createStartOrStopKeyForIndexScan(filterColsDetails, commonKey, startRow, true));
    // Find the end key for the scan
    scan.setStopRow(createStartOrStopKeyForIndexScan(filterColsDetails, commonKey, stopRow, false));
    return scan;
  }

  private byte[] createCommonKeyForIndex(byte[] regionStartKey, byte[] indexName) {
    // Format for index table rowkey [Startkey for the index region] + [one 0 byte]+
    // [Index name] + [Padding for the max index name] + ....
    int commonKeyLength = regionStartKey.length + 1 + IndexUtils.getMaxIndexNameLength();
    ByteArrayBuilder builder = ByteArrayBuilder.allocate(commonKeyLength);
    // Adding the startkey for the index region and single 0 Byte.
    builder.put(regionStartKey);
    builder.position(builder.position() + 1);
    // Adding the index name and the padding needed
    builder.put(indexName);
    // No need to add the padding bytes specifically. In the array all the bytes will be 0s.
    return builder.array();
  }

  // When it comes here, only the last column in the colDetails will be a range.
  // Others will be exact value. EQUALS condition.
  private byte[] createStartOrStopKeyForIndexScan(List<FilterColumnValueDetail> colDetails,
      byte[] commonKey, byte[] userTabRowKey, boolean isStartKey) {
    int totalValueLen = 0;
    for (FilterColumnValueDetail fcvd : colDetails) {
      totalValueLen += fcvd.maxValueLength;
    }
    int userTabRowKeyLen = userTabRowKey == null ? 0 : userTabRowKey.length;
    ByteArrayBuilder builder =
        ByteArrayBuilder.allocate(commonKey.length + totalValueLen + userTabRowKeyLen);
    builder.put(commonKey);
    for (int i = 0; i < colDetails.size(); i++) {
      FilterColumnValueDetail fcvd = colDetails.get(i);
      if (i == colDetails.size() - 1) {
        // This is the last column in the colDetails. Here the range check can come
        if (isStartKey) {
          if (fcvd.compareOp == null) {
            // This can happen when the condition is a range condition and only upper bound is
            // specified. ie. c1<100
            // Now the column value can be specified as 0 bytes. Just we need to advance the byte[]
            assert fcvd instanceof FilterColumnValueRange;
            assert fcvd.value == null;
            builder.position(builder.position() + fcvd.maxValueLength);
          } else if (fcvd.compareOp.equals(CompareOp.EQUAL)
              || fcvd.compareOp.equals(CompareOp.GREATER_OR_EQUAL)) {
            copyColumnValueToKey(builder, fcvd.value, fcvd.maxValueLength, fcvd.valueType);
          } else if (fcvd.compareOp.equals(CompareOp.GREATER)) {
            // We can go with the value + 1
            // For eg : if type is int and value is 45, make startkey considering value as 46
            // If type is String and value is 'ad' make startkey considering value as 'ae'

            copyColumnValueToKey(builder, fcvd.value, fcvd.maxValueLength, fcvd.valueType);
            IndexUtils.incrementValue(builder.array(), false);
          }
        } else {
          CompareOp op = fcvd.compareOp;
          byte[] value = fcvd.value;
          if (fcvd instanceof FilterColumnValueRange) {
            op = ((FilterColumnValueRange) fcvd).getUpperBoundCompareOp();
            value = ((FilterColumnValueRange) fcvd).getUpperBoundValue();
          }
          if (op == null) {
            // This can happen when the condition is a range condition and only lower bound is
            // specified. ie. c1>=10
            assert fcvd instanceof FilterColumnValueRange;
            assert value == null;
            // Here the stop row is already partially built. As there is no upper bound all the
            // possibles values for that column we need to consider. Well the max value for a
            // column with maxValueLength=10 will a byte array of 10 bytes with all bytes as FF.
            // But we can put this FF bytes into the stop row because the stop row will not be
            // considered by the scanner. So we need to increment this by 1.
            // We can increment the byte[] created until now by 1.
            byte[] stopRowTillNow = builder.array(0, builder.position());
            stopRowTillNow = IndexUtils.incrementValue(stopRowTillNow, true);
            // Now we need to copy back this incremented value to the builder.
            builder.position(0);
            builder.put(stopRowTillNow);
            // Now just advance the builder pos by fcvd.maxValueLength as we need all 0 bytes
            builder.position(builder.position() + fcvd.maxValueLength);
          } else if (op.equals(CompareOp.EQUAL) || op.equals(CompareOp.LESS_OR_EQUAL)) {
            copyColumnValueToKey(builder, value, fcvd.maxValueLength, fcvd.valueType);
            IndexUtils.incrementValue(builder.array(), false);

          } else if (op.equals(CompareOp.LESS)) {
            copyColumnValueToKey(builder, value, fcvd.maxValueLength, fcvd.valueType);
          }
        }
      } else {
        // For all other columns other than the last column the condition must be EQUALS
        copyColumnValueToKey(builder, fcvd.value, fcvd.maxValueLength, fcvd.valueType);
      }
    }
    if (userTabRowKeyLen > 0) {
      builder.put(userTabRowKey);
    }

    return builder.array();
  }

  private void copyColumnValueToKey(ByteArrayBuilder builder, byte[] colValue, int maxValueLength,
      ValueType valueType) {
    colValue = IndexUtils.changeValueAccToDataType(colValue, valueType);
    builder.put(colValue);
    int paddingLength = maxValueLength - colValue.length;
    builder.position(builder.position() + paddingLength);
  }

  // This method will do the reorder and restructuring of the filters so as the finding of
  // suitable index for the query will be easier for the remaining steps
  // This will do 2 things basically
  // 1.Group the AND condition on SingleColumnValueFilter together so as to make a range condition.
  // In this process it will validate the correctness of the range and will avoid the invalid
  // things.
  // 2.Move the adjacent AND conditions together within on filter list. If N number of adjacent
  // filters or filter lists are there and all are combined using AND condition then it is same
  // as all within one list.
  // Default access for testability
  Filter doFiltersRestruct(Filter filter) {
    if (filter instanceof SingleColumnValueFilter) {
      ValuePartition vp = null;
      if (filter instanceof SingleColumnValuePartitionFilter) {
        vp = ((SingleColumnValuePartitionFilter) filter).getValuePartition();
      }
      SingleColumnValueFilter scvf = (SingleColumnValueFilter) filter;
      if (scvf.getOperator().equals(CompareOp.LESS)
          || scvf.getOperator().equals(CompareOp.LESS_OR_EQUAL)
          || scvf.getOperator().equals(CompareOp.GREATER)
          || scvf.getOperator().equals(CompareOp.GREATER_OR_EQUAL)) {
        return new SingleColumnRangeFilter(scvf.getFamily(), scvf.getQualifier(), vp, scvf
            .getComparator().getValue(), scvf.getOperator(), null, null);
      }
    }
    FilterGroupingWorker groupWorker = new FilterGroupingWorker();
    return groupWorker.group(filter);
  }

  // For a multi column index, the index look up with a value range can be supported only on the
  // last column. The previous columns lookups to be exact value look up.
  // Index idx1 on columns c1 and c2. A lookup condition with c1=x and c2 btw x and z can be
  // supported using the index idx1. But if the lookup is c1 btw a and k and c2... we can better
  // not support using idx1 there. (May be check later we can support this)
  // In this case 2 indices idx1 and idx2 on col1 and col2 respectively can be used.
  // In the above case a lookup only on c1, either it is a range lookup or single value lookup
  // we can make use of the multi column index on c1 and c2.
  // In this case if there is an index on c1 alone, we better use that.
  // But a single column lookup on column c1 and there is an index on c2 and c1 , we can not use
  // this index. It is equivalent to c1=x and c2 btw min to max.
  // Default access for testability
  FilterNode evalFilterForIndexSelection(Filter filter, List<IndexSpecification> indices) {
    if (filter instanceof FilterList) {
      FilterList fList = (FilterList) filter;
      GroupingCondition condition =
          (fList.getOperator() == Operator.MUST_PASS_ALL) ? GroupingCondition.AND
              : GroupingCondition.OR;
      NonLeafFilterNode nonLeafFilterNode = new NonLeafFilterNode(condition);
      List<Filter> filters = fList.getFilters();
      for (Filter fltr : filters) {
        FilterNode node = evalFilterForIndexSelection(fltr, indices);
        nonLeafFilterNode.addFilterNode(node);
      }
      return handleNonLeafFilterNode(nonLeafFilterNode);
    } else if (filter instanceof SingleColumnValueFilter) {
      // Check for the availability of index
      return selectBestFitAndPossibleIndicesForSCVF(indices, (SingleColumnValueFilter) filter);
    } else if (filter instanceof SingleColumnRangeFilter) {
      return selectBestFitAndPossibleIndicesForSCRF(indices, (SingleColumnRangeFilter) filter);
    }
    return new NoIndexFilterNode();
  }

  private FilterNode selectBestFitAndPossibleIndicesForSCRF(List<IndexSpecification> indices,
      SingleColumnRangeFilter filter) {
    FilterColumnValueRange range =
        new FilterColumnValueRange(filter.getFamily(), filter.getQualifier(),
            filter.getValuePartition(), filter.getLowerBoundValue(), filter.getLowerBoundOp(),
            filter.getUpperBoundValue(), filter.getUpperBoundOp());
    return selectBestFitIndexForColumn(indices, range);
  }

  private FilterNode handleNonLeafFilterNode(NonLeafFilterNode nonLeafFilterNode) {
    // check whether this node can be changed to a NoIndexFilterNode
    // This we can do when the condition in nonLeafFilterNode is OR and any of the
    // child node is NoIndexFilterNode
    if (nonLeafFilterNode.getGroupingCondition() == GroupingCondition.OR) {
      return handleORCondition(nonLeafFilterNode);
    } else {
      // AND condition
      return handleANDCondition(nonLeafFilterNode);
    }
  }

  private FilterNode handleORCondition(NonLeafFilterNode nonLeafFilterNode) {
    Iterator<FilterNode> nonLeafFilterNodeItr = nonLeafFilterNode.getFilterNodes().iterator();
    while (nonLeafFilterNodeItr.hasNext()) {
      FilterNode filterNode = nonLeafFilterNodeItr.next();
      if (filterNode instanceof IndexFilterNode) {
        FilterColumnValueDetail filterColumnValueDetail =
            ((IndexFilterNode) filterNode).getFilterColumnValueDetail();
        IndexSpecification indexToUse = ((IndexFilterNode) filterNode).getBestIndex();
        nonLeafFilterNode.addIndicesToUse(filterColumnValueDetail, indexToUse);
        nonLeafFilterNodeItr.remove();
      } else if (filterNode instanceof PossibleIndexFilterNode
          || filterNode instanceof NoIndexFilterNode) {
        // The moment an OR condition contains a column on which there is no index which can be
        // used, the entire OR node becomes as non indexed.
        return new NoIndexFilterNode();
      }
      // A NonLeafFilterNode under the OR node need to be kept as it is.
    }
    return nonLeafFilterNode;
  }

  private FilterNode handleANDCondition(NonLeafFilterNode nonLeafFilterNode) {
    Map<Column, LeafFilterNode> leafNodes = new HashMap<Column, LeafFilterNode>();
    Iterator<FilterNode> filterNodesIterator = nonLeafFilterNode.getFilterNodes().iterator();
    while (filterNodesIterator.hasNext()) {
      FilterNode filterNode = filterNodesIterator.next();
      if (filterNode instanceof LeafFilterNode) {
        LeafFilterNode lfNode = (LeafFilterNode) filterNode;
        leafNodes.put(lfNode.getFilterColumnValueDetail().column, lfNode);
        filterNodesIterator.remove();
      } else if (filterNode instanceof NoIndexFilterNode) {
        filterNodesIterator.remove();
      }
      // Any NonLeafFilterNode under this NonLeafFilterNode will be kept as it is.
      // This will be a OR condition corresponding node.
    }
    // This below method will consider all the leafNodes just under that and will try to
    // finalize one or more index to be used for those col. It will try to get the best
    // combination minimizing the number of indices to be used. If I have say 5 leaf cols
    // under this AND node and there is one index on these 5 cols, well I can use that one
    // index. If not will try to find indices which can be applied on the subsets of these
    // 5 cols, say one on 3 cols and other on 2 cols
    if (!leafNodes.isEmpty()) {
      Map<List<Column>, IndexSpecification> colVsIndex = finalizeIndexForLeafNodes(leafNodes);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Index(s) which will be used for columns " + leafNodes.keySet() + " : "
            + colVsIndex);
      }
      if (colVsIndex != null) {
        addIndicesToNonLeafAndNode(colVsIndex, nonLeafFilterNode, leafNodes);
      }
    }
    return nonLeafFilterNode;
  }

  // Here also 2 many loops we can avoid this
  @SuppressWarnings("unchecked")
  private Map<List<Column>, IndexSpecification> finalizeIndexForLeafNodes(
      Map<Column, LeafFilterNode> leafNodes) {
    // go with the breakups and check
    // suppose there are 5 cols under the AND condition and are c1,c2,c3,c4,c5
    // There can be different break ups for the cols possible.
    // [5],[4,1],[3,2],[3,1,1],[2,2,1],[2,1,1,1],[1,1,1,1,1]
    // In each of these breakup also we can get many columns combinations.
    // Except in first and last where either all cols in one group or 1 column only.
    // For [4,1] there can be 5c1 combinations possible.
    Set<List<List<List<Column>>>> colBreakUps = getColsBreakUps(leafNodes.keySet());
    ColBreakUpIndexDetails bestColBreakUpIndexDetails = null;
    for (List<List<List<Column>>> colBreakUp : colBreakUps) {
      ColBreakUpIndexDetails colBreakUpIndexDetails =
          findBestIndicesForColSplitsInBreakUp(colBreakUp, leafNodes);
      if (colBreakUpIndexDetails == null) {
        continue;
      }
      if (colBreakUpIndexDetails.isBestIndex) {
        // This means this is THE best index. It solves all the columns and exactly those cols only
        // there as part of the indices too.. What else we need...
        bestColBreakUpIndexDetails = colBreakUpIndexDetails;
        break;
      } else {
        if (bestColBreakUpIndexDetails == null
            || isIndicesGroupBetterThanCurBest(colBreakUpIndexDetails, bestColBreakUpIndexDetails)) {
          bestColBreakUpIndexDetails = colBreakUpIndexDetails;
        }
      }
    }
    // TODO some more logging of the output..
    return bestColBreakUpIndexDetails == null ? null
        : bestColBreakUpIndexDetails.bestIndicesForBreakUp;
  }

  private void addIndicesToNonLeafAndNode(Map<List<Column>, IndexSpecification> colsVsIndex,
      NonLeafFilterNode nonLeafFilterNode, Map<Column, LeafFilterNode> leafNodes) {
    for (Entry<List<Column>, IndexSpecification> entry : colsVsIndex.entrySet()) {
      List<Column> cols = entry.getKey();
      int colsSize = cols.size();
      IndexSpecification index = entry.getValue();
      // The FilterColumnValueDetail for cols need to be in the same order as that of cols
      // in the index. This order will be important for creating the start/stop keys for
      // index scan.
      List<FilterColumnValueDetail> fcvds = new ArrayList<FilterColumnValueDetail>(colsSize);
      int i = 0;
      for (ColumnQualifier cq : index.getIndexColumns()) {
        FilterColumnValueDetail fcvd =
            leafNodes.get(
              new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition()))
                .getFilterColumnValueDetail();
        assert fcvd != null;
        fcvds.add(fcvd);
        i++;
        if (i == colsSize) {
          // The selected index might be on more cols than those we are interested in now.
          // All those will be towards the end.
          break;
        }
      }
      LOG.info("Index using for the columns " + cols + " : " + index);
      nonLeafFilterNode.addIndicesToUse(fcvds, index);
    }
  }

  private static class ColBreakUpIndexDetails {
    private Map<List<Column>, IndexSpecification> bestIndicesForBreakUp;
    private int bestIndicesCardinality = -1;
    private int colsResolvedByBestIndices = -1;
    private boolean isBestIndex = false;
  }

  private ColBreakUpIndexDetails findBestIndicesForColSplitsInBreakUp(
      List<List<List<Column>>> colBreakUpCombs, Map<Column, LeafFilterNode> leafNodes) {
    int totalColsInBreakup = leafNodes.size();
    ColBreakUpIndexDetails bestColBreakUpIndexDetails = null;
    // List<List<List<Column>>> breakUp contains different combinations of the columns in a
    // particular break up case. Say for 3 cols, a,b,c and the breakup is [2,1], this list
    // contains combinations [[a,b],[c]],[[b,c],[a]],[[a,c],[b]]
    for (List<List<Column>> colBreakUpComb : colBreakUpCombs) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking for the best index(s) for the cols combination : " + colBreakUpComb);
      }
      Map<List<Column>, IndexSpecification> colsVsIndex =
          new HashMap<List<Column>, IndexSpecification>();
      for (List<Column> cols : colBreakUpComb) {
        IndexSpecification index = findBestIndex(cols, leafNodes);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Best index selected for the cols " + cols + " : " + index);
        }
        if (index != null) {
          colsVsIndex.put(cols, index);
        }
      }
      if (colsVsIndex.size() == 0) {
        // For this cols breakup not even single index we are able to find which solves atleast
        // one column. Just continue with the next combination.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not even one index found for the cols combination : " + colBreakUpComb);
        }
        continue;
      }
      int netIndicesCardinality = 0;
      int colsResolved = 0;
      for (Entry<List<Column>, IndexSpecification> entry : colsVsIndex.entrySet()) {
        colsResolved += entry.getKey().size();
        netIndicesCardinality += entry.getValue().getIndexColumns().size();
      }
      // Am I THE real best combination
      if (colBreakUpComb.size() == colsVsIndex.size()) {
        assert colsResolved == totalColsInBreakup;
        // Means we have index for all the cols combinations
        // Now check the net cardinality of all the indices
        if (netIndicesCardinality == totalColsInBreakup) {
          // This is the best
          LOG.info("Got indices combination for the cols " + colsVsIndex + " which is THE BEST!");
          ColBreakUpIndexDetails bestDetails = new ColBreakUpIndexDetails();
          bestDetails.bestIndicesForBreakUp = colsVsIndex;
          bestDetails.isBestIndex = true;
          // colsResolved and netIndicesCardinality not needed in this case
          return bestDetails;
        }
      }
      // Checking whether this colsIndexMap combination better than the previous best.
      ColBreakUpIndexDetails curDetails = new ColBreakUpIndexDetails();
      curDetails.bestIndicesForBreakUp = colsVsIndex;
      curDetails.colsResolvedByBestIndices = colsResolved;
      curDetails.bestIndicesCardinality = netIndicesCardinality;
      if (bestColBreakUpIndexDetails == null
          || isIndicesGroupBetterThanCurBest(curDetails, bestColBreakUpIndexDetails)) {
        bestColBreakUpIndexDetails = curDetails;
      }
    }
    return bestColBreakUpIndexDetails;
  }

  private boolean isIndicesGroupBetterThanCurBest(ColBreakUpIndexDetails curColBreakUpIndexDetails,
      ColBreakUpIndexDetails bestColBreakUpIndexDetails) {
    // Conditions to decide whether current one is better than the current best
    // 1. The total number of cols all the indices resolve
    // 2. The number of indices which resolves the cols
    // 3. The net cardinality of all the indices
    // TODO later can check for the dynamic metric data about indices and decide which can be used.
    int colsResolvedByCurIndices = curColBreakUpIndexDetails.colsResolvedByBestIndices;
    int colsResolvedByBestIndices = bestColBreakUpIndexDetails.colsResolvedByBestIndices;
    if (colsResolvedByCurIndices > colsResolvedByBestIndices) {
      // The more cols it resolves the better.
      return true;
    } else if (colsResolvedByCurIndices == colsResolvedByBestIndices) {
      int indicesInCurComb = curColBreakUpIndexDetails.bestIndicesForBreakUp.size();
      int indicesInBestComb = bestColBreakUpIndexDetails.bestIndicesForBreakUp.size();
      if (indicesInCurComb < indicesInBestComb) {
        // The lesser the number of indices the better.
        return true;
      } else if (indicesInCurComb == indicesInBestComb) {
        int curIndicesCardinality = curColBreakUpIndexDetails.bestIndicesCardinality;
        int bestIndicesCardinality = bestColBreakUpIndexDetails.bestIndicesCardinality;
        if (curIndicesCardinality < bestIndicesCardinality) {
          // The lesser the cardinality the better.
          return true;
        }
      }
    }
    return false;
  }

  private IndexSpecification
      findBestIndex(List<Column> cols, Map<Column, LeafFilterNode> leafNodes) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to find a best index for the cols : " + cols);
    }
    Set<IndexSpecification> indicesToUse = getPossibleIndicesForCols(cols, leafNodes);
    // indicesToUse will never come as null....
    if (LOG.isDebugEnabled()) {
      LOG.debug("Possible indices for cols " + cols + " : " + indicesToUse);
    }
    IndexSpecification bestIndex = null;
    int bestIndexCardinality = -1;
    for (IndexSpecification index : indicesToUse) {
      if (isIndexSuitable(index, cols, leafNodes)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Index " + index + " seems to be suitable for the columns " + cols);
        }
        if (index.getIndexColumns().size() == cols.size()) {
          // Yea we got the best index. Juts return this. No need to loop through and check
          // with other indices
          return index;
        }
        // Compare this index with the current best. This will be better if its cardinality
        // is better(lesser) than the current best's
        // TODO pluggable interface to decide which index to be used when both this and current
        // best index having same cardinality.
        if (bestIndex == null || index.getIndexColumns().size() < bestIndexCardinality) {
          bestIndex = index;
          bestIndexCardinality = index.getIndexColumns().size();
        }
      }
    }
    return bestIndex;
  }

  private Set<IndexSpecification> getPossibleIndicesForCols(List<Column> cols,
      Map<Column, LeafFilterNode> leafNodes) {
    // When there are more than one range conditioned columns, no need to check with any of the
    // index. We will not get any matched index for all cols.
    int noOfRangeConds = 0;
    for (Column col : cols) {
      LeafFilterNode leafFilterNode = leafNodes.get(col);
      if (leafFilterNode != null
          && leafFilterNode.getFilterColumnValueDetail() instanceof FilterColumnValueRange) {
        noOfRangeConds++;
      }
      if (noOfRangeConds > 1) {
        return EMPTY_INDEX_SET;
      }
    }
    // For a given column with a range condition an index can be its possible index or
    // future possible index iff this column is the last column in the index
    // Suppose an index on c1&c2 and the column c1 is having of range 10-20, then this index
    // can not be used for this range condition. [provided some condition is there on c2 also]
    // AND(c1=10,c2=20,c3 btw[10,20])
    Set<IndexSpecification> indicesToUse = new HashSet<IndexSpecification>();
    for (Column col : cols) {
      LeafFilterNode lfn = leafNodes.get(col);
      if (lfn != null) {
        FilterColumnValueDetail filterColumnValueDetail = lfn.getFilterColumnValueDetail();
        IndexSpecification bestIndex = lfn.getBestIndex();
        if (bestIndex != null) {
          indicesToUse.add(bestIndex);
        }
        Map<Column, List<Pair<IndexSpecification, Integer>>> possibleUseIndices =
            lfn.getPossibleUseIndices();
        if (possibleUseIndices != null) {
          List<Pair<IndexSpecification, Integer>> possibleIndices =
              possibleUseIndices.get(filterColumnValueDetail.getColumn());
          if (possibleIndices != null) {
            for (Pair<IndexSpecification, Integer> pair : possibleIndices) {
              indicesToUse.add(pair.getFirst());
            }
          }
        }
        Map<Column, List<Pair<IndexSpecification, Integer>>> possibleFutureUseIndices =
            lfn.getPossibleFutureUseIndices();
        if (possibleFutureUseIndices != null) {
          List<Pair<IndexSpecification, Integer>> possibleIndices =
              possibleFutureUseIndices.get(filterColumnValueDetail.getColumn());
          if (possibleIndices != null) {
            for (Pair<IndexSpecification, Integer> pair : possibleIndices) {
              indicesToUse.add(pair.getFirst());
            }
          }
        }
      }
    }
    return indicesToUse;
  }

  // Whether the passed index is suitable to be used for the given columns.
  // index is suitable when the it contains all the columns.
  // Also index should not contain any other column before any of the column
  // in the passed cols list
  private boolean isIndexSuitable(IndexSpecification index, List<Column> cols,
      Map<Column, LeafFilterNode> leafNodes) {
    int matchedCols = 0;
    for (ColumnQualifier cq : index.getIndexColumns()) {
      Column column = new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition());
      if (cols.contains(column)) {
        matchedCols++;
        // leafNodes.get(column) will never be null.. Don't worry
        if (leafNodes.get(column).getFilterColumnValueDetail() instanceof FilterColumnValueRange) {
          // When the condition on the column is a range condition, we need to ensure in this index
          // 1. The column is the last column
          // or
          // 2. There are no columns in this index which is part of the cols list
          if (matchedCols != cols.size()) {
            return false;
          }
        }
      } else {
        if (matchedCols != cols.size()) {
          return false;
        }
      }
      if (matchedCols == cols.size()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  Set<List<List<List<Column>>>> getColsBreakUps(Set<Column> columns) {
    int totalColsCount = columns.size();
    // Breakups should be sorted such that the best one come before the others. The best one is the
    // one with lower number of splits. If the number of splits same, then the which contains more
    // cols in one split.
    // eg: When there are total 5 cols, then split [7] is better than [4,1] and [4,1] is better
    // than [3,2]
    Comparator<List<List<List<Column>>>> comparator = new Comparator<List<List<List<Column>>>>() {
      @Override
      public int compare(List<List<List<Column>>> l1, List<List<List<Column>>> l2) {
        List<List<Column>> firstColsCombl1 = l1.get(0);
        List<List<Column>> firstColsCombl2 = l2.get(0);
        int result = firstColsCombl1.size() - firstColsCombl2.size();
        if (result != 0) return result;
        int size = Math.min(firstColsCombl1.size(), firstColsCombl2.size());
        for (int i = 0; i < size; i++) {
          int colsSizeInSplit1 = firstColsCombl1.get(i).size();
          int colsSizeInSplit2 = firstColsCombl2.get(i).size();
          result = colsSizeInSplit1 - colsSizeInSplit2;
          if (result != 0) return -result;
        }
        return 0;
      }
    };
    Set<List<List<List<Column>>>> listOfCols = new TreeSet<List<List<List<Column>>>>(comparator);
    ColCombination comb = new ColCombination();
    ArrayList<Integer> breakUp = null;
    int firstItemColsCount = totalColsCount;
    while (firstItemColsCount >= 1) {
      breakUp = new ArrayList<Integer>();
      breakUp.add(firstItemColsCount);
      int totalColsCountInBreakUp = firstItemColsCount;
      while (totalColsCountInBreakUp < totalColsCount) {
        // Fill with 1...
        breakUp.add(1);
        totalColsCountInBreakUp++;
      }
      listOfCols.add(makeColCombination(columns, breakUp, comb));
      // now try to combine the after 1st entry 1s into bigger items.
      // But this combined value never should be more than the 1st value.
      // group last 2 items and create a new breakup..
      while (true) {
        // In order to do this group there should be atleast 3 elements in the breakUp list.
        if (breakUp.size() < 3) {
          break;
        }
        breakUp = (ArrayList<Integer>) breakUp.clone();
        int lastElem = breakUp.remove(breakUp.size() - 1);
        int secondLastElem = breakUp.remove(breakUp.size() - 1);
        // Add this element to the current last. ie. the old second last element
        // Well this element must be 1 only.
        int newLastElem = lastElem + secondLastElem;
        if (newLastElem > firstItemColsCount) {
          break;
        }
        breakUp.add(newLastElem);
        listOfCols.add(makeColCombination(columns, breakUp, comb));
      }
      firstItemColsCount--;
    }
    return listOfCols;
  }

  private List<List<List<Column>>> makeColCombination(Set<Column> columns, List<Integer> breakUp,
      ColCombination comb) {
    int size = breakUp.size();
    return comb.formColCombinations(size, columns, breakUp);
  }

  /**
   * Generates the column combination
   */
  private static class ColCombination {

    public List<List<List<Column>>> formColCombinations(int breakUpSize, Set<Column> columns,
        List<Integer> breakUp) {
      // If no of cols and break up is same it will be all 1s combination. So just return the list
      List<List<List<Column>>> finalCols = new ArrayList<List<List<Column>>>();
      ArrayList<List<Column>> possibleCols = new ArrayList<List<Column>>();
      LinkedList<Column> copyOfColumns = new LinkedList<Column>(columns);
      int lastIndex = 0;
      boolean allOnes = false;
      // for every break up find the combination
      for (Integer r : breakUp) {
        if (possibleCols.size() == 0) {
          possibleCols =
              (ArrayList<List<Column>>) createCombination(copyOfColumns, r, possibleCols);
          copyOfColumns = new LinkedList<Column>(columns);
          if (columns.size() == breakUpSize) {
            // for [1 1 1 1 1]
            allOnes = true;
            break;
          }
        } else {
          int colSize = possibleCols.size();
          List<List<Column>> cloneOfPossibleCols = new ArrayList<List<Column>>(possibleCols);
          List<List<Column>> possibleCols1 = null;
          for (int i = lastIndex; i < colSize; i++) {
            possibleCols1 = new ArrayList<List<Column>>();
            List<Column> col = clone(cloneOfPossibleCols, i);
            copyOfColumns.removeAll(cloneOfPossibleCols.get(i));
            possibleCols1 = createCombination(copyOfColumns, r, possibleCols);

            copyOfColumns = new LinkedList<Column>(columns);
            boolean cloned = true;
            for (List<Column> set : possibleCols1) {
              if (cloned == false) {
                col = clone(cloneOfPossibleCols, i);
              }
              col.addAll(set);
              possibleCols.add(col);

              cloned = false;
            }
          }
          lastIndex = colSize;
        }
      }
      // Optimize here
      if (!allOnes) {
        for (int i = lastIndex; i < possibleCols.size(); i++) {
          List<List<Column>> subList = new ArrayList<List<Column>>();
          int prev = 0;
          for (int j = 0; j < breakUp.size(); j++) {
            int index = breakUp.get(j);
            subList.add(possibleCols.get(i).subList(prev, prev + index));
            prev = prev + index;

          }
          finalCols.add(subList);
        }
      } else {
        finalCols.add(possibleCols);
      }
      return finalCols;
    }

    private List<Column> clone(List<List<Column>> cloneOfPossibleCols, int i) {
      return (List<Column>) ((ArrayList<Column>) cloneOfPossibleCols.get(i)).clone();
    }

    private List<List<Column>> createCombination(LinkedList<Column> copyOfColumns, int r,
        ArrayList<List<Column>> possibleCols2) {
      int j;
      int k = 0;
      List<List<Column>> possibleCols = new ArrayList<List<Column>>();
      ArrayList<Column> colList = new ArrayList<Column>();
      int size = copyOfColumns.size();
      for (int i = 0; i < (1 << size); i++) {
        if (r != findNoOfBitsSet(i)) {
          continue;
        }
        j = i;
        k = 0;
        while (j != 0) {
          if (j % 2 != 0) {
            colList.add(copyOfColumns.get(k));
          }
          j /= 2;
          k++;
        }
        possibleCols.add((List<Column>) colList.clone());
        colList.clear();
      }
      return possibleCols;
    }

    private int findNoOfBitsSet(int n) {
      int count = 0;
      while (n != 0) {
        n &= n - 1;
        count++;
      }
      return count;
    }
  }

  // This is at the ColumnValueFilter level. That means value for one cf:qualifier(let me call
  // this column as col1) is provided in the condition
  // Any index on the table which contain the column col1 as the 1st index column in it can be
  // selected for the usage. If the table having an index on col1 & col2 we can consider this
  // index as col1 is the 1st column in the index columns list. But if the index is on col2 & col1
  // ie. 1st col in the index cols list is not col1, we wont be able to use that index.
  // Now comes the question of best fit index. When there are more than one possible index to be
  // used, we need to go with one index. Others are only possible candidates to be considered.
  // Suppose there is an index on col1&col2 and another on col1 alone. Here to consider the 2nd
  // index will be better as that will be having lesser data in the index table, that we need to
  // scan. So exact match is always better.
  // Suppose there is an index on col1&col2 and another on col1&col2&col3, it this case it would
  // be better to go with index on col1&col2 as this will be giving lesser data to be scanned.
  // To be general, if there are more possible indices which can be used, we can select the one
  // which is having lesser number of index cols in it.
  private FilterNode selectBestFitAndPossibleIndicesForSCVF(List<IndexSpecification> indices,
      SingleColumnValueFilter filter) {
    if (CompareOp.NOT_EQUAL == filter.getOperator() || CompareOp.NO_OP == filter.getOperator()) {
      return new NoIndexFilterNode();
    }
    FilterColumnValueDetail detail = null;
    if (filter instanceof SingleColumnValuePartitionFilter) {
      SingleColumnValuePartitionFilter escvf = (SingleColumnValuePartitionFilter) filter;
      detail =
          new FilterColumnValueDetail(escvf.getFamily(), escvf.getQualifier(), escvf
              .getComparator().getValue(), escvf.getValuePartition(), escvf.getOperator());
    } else {
      detail =
          new FilterColumnValueDetail(filter.getFamily(), filter.getQualifier(), filter
              .getComparator().getValue(), filter.getOperator());
    }
    return selectBestFitIndexForColumn(indices, detail);
  }

  private FilterNode selectBestFitIndexForColumn(List<IndexSpecification> indices,
      FilterColumnValueDetail detail) {
    FilterNode filterNode =
        this.colFilterNodeCache.get(new ColumnWithValue(detail.getColumn(), detail.getValue()));
    if (filterNode != null) {
      // This column is being already evaluated to find the best an possible indices
      // No need to work on this
      return filterNode;
    }
    List<Pair<IndexSpecification, Integer>> possibleUseIndices =
        new ArrayList<Pair<IndexSpecification, Integer>>();
    List<Pair<IndexSpecification, Integer>> possibleFutureUseIndices =
        new ArrayList<Pair<IndexSpecification, Integer>>();
    IndexSpecification bestFitIndex = null;
    int bestFitIndexColsSize = Integer.MAX_VALUE;
    for (IndexSpecification index : indices) {
      Set<ColumnQualifier> indexColumns = index.getIndexColumns();
      // Don't worry . This Set is LinkedHashSet. So this will maintain the order.
      Iterator<ColumnQualifier> indexColumnsIterator = indexColumns.iterator();
      ColumnQualifier firstCQInIndex = indexColumnsIterator.next();
      Column firstColInIndex =
          new Column(firstCQInIndex.getColumnFamily(), firstCQInIndex.getQualifier(),
              firstCQInIndex.getValuePartition());
      if (firstColInIndex.equals(detail.column)) {
        possibleUseIndices.add(new Pair<IndexSpecification, Integer>(index, indexColumns.size()));
        // When we have condition on col1 and we have indices on col1&Col2 and col1&col3
        // which one we should select? We dont know the data related details of every index.
        // So select any one. May be the 1st come selected.
        // TODO later we can add more intelligence and then add index level data details
        if (indexColumns.size() < bestFitIndexColsSize) {
          bestFitIndex = index;
          bestFitIndexColsSize = indexColumns.size();
        }
        detail.maxValueLength = firstCQInIndex.getMaxValueLength();
        detail.valueType = firstCQInIndex.getType();
      }
      // This index might be useful at a topper level....
      // I will explain in detail
      // When the filter from customer is coming this way as shown below
      // &
      // ___|___
      // | |
      // c1=10 c2=5
      // Suppose we have an index on c2&c1 only on the table, when we check for c1=10 node
      // we will not find any index for that as index is not having c1 as the 1st column.
      // For c2=5 node, the index is a possible option. Now when we consider the & node,
      // the index seems perfect match. That is why even for the c1=10 node also, we can not
      // sat it is a NoIndexFilterNode, if there are any indices on the table which contain c1
      // as one column in the index columns.
      else {
        ColumnQualifier cq = null;
        Column column = null;
        while (indexColumnsIterator.hasNext()) {
          cq = indexColumnsIterator.next();
          column = new Column(cq.getColumnFamily(), cq.getQualifier(), cq.getValuePartition());
          if (column.equals(detail.column)) {
            possibleFutureUseIndices.add(new Pair<IndexSpecification, Integer>(index, indexColumns
                .size()));
            detail.maxValueLength = firstCQInIndex.getMaxValueLength();
            detail.valueType = firstCQInIndex.getType();
            break;
          }
        }
      }
    }
    if (null != bestFitIndex) {
      filterNode =
          new IndexFilterNode(bestFitIndex, possibleUseIndices, possibleFutureUseIndices, detail);
    } else if (!possibleFutureUseIndices.isEmpty()) {
      // when bestFitIndex is null possibleUseIndices will be empty for sure
      filterNode = new PossibleIndexFilterNode(possibleFutureUseIndices, detail);
    } else {
      filterNode = new NoIndexFilterNode();
    }
    byte[] val = null;
    if (detail instanceof FilterColumnValueRange) {
      // Probably we have LESS condition
      if (detail.getValue() == null) {
        val = ((FilterColumnValueRange) detail).getUpperBoundValue();
      } else {
        val = detail.getValue();
      }
    }
    this.colFilterNodeCache.put(new ColumnWithValue(detail.getColumn(), val), filterNode);
    return filterNode;
  }

  // TODO toString() in the VO classes

  public static class ColumnWithValue {
    private Column column;
    private byte[] value;

    public ColumnWithValue(Column column, byte[] value) {
      this.column = column;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ColumnWithValue)) return false;
      ColumnWithValue that = (ColumnWithValue) obj;
      Column col = that.getColumn();
      boolean equals = this.column.equals(col);
      if (equals && Bytes.equals(this.value, that.getValue())) {
        return true;
      } else {
        return false;
      }
    }

    public Column getColumn() {
      return this.column;
    }

    public byte[] getValue() {
      return this.value;
    }

    @Override
    public int hashCode() {
      return this.column.hashCode();
    }
  }
}