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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.SplitTransaction.SplitInfo;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This is an extension for the RegionObserver interface. The APIs added into this interface are not
 * exposed by HBase. This is internally being used by CMWH HBase. Customer should not make use of
 * this interface points. <br>
 * Note : The APIs in this interface is subject to change at any time.
 */
public interface RegionObserverExt {

  /**
   * Internally the Put/Delete are handled as a batch Called before actual put operation starts in
   * the region.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained coprocessors
   * @param ctx the environment provided by the region server
   * @param mutations list of mutations
   * @param edit The WALEdit object that will be written to the wal
   * @throws IOException if an error occurred on the coprocessor
   */
  void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final List<Pair<Mutation, OperationStatus>> mutationVsBatchOp, final WALEdit edit)
      throws IOException;

  /**
   * Internally the Put/Delete are handled as a batch. Called after actual batch put operation
   * completes in the region.
   * <p>
   * Call CoprocessorEnvironment#bypass to skip default actions
   * <p>
   * Call CoprocessorEnvironment#complete to skip any subsequent chained coprocessors
   * @param ctx the environment provided by the region server
   * @param mutations list of mutations
   * @param walEdit The WALEdit object that will be written to the wal
   */
  void postBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      final List<Mutation> mutations, WALEdit walEdit) throws IOException;

  /**
   * Called after the completion of batch put/delete and will be called even if the batch operation
   * fails
   * @param ctx
   * @param mutations list of mutations
   * @throws IOException
   */
  void postCompleteBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Mutation> mutations) throws IOException;

  /**
   * This will be called before PONR step as part of split transaction
   * @param ctx
   * @param splitKey
   * @return
   * @throws IOException
   */
  SplitInfo preSplitBeforePONR(final ObserverContext<RegionCoprocessorEnvironment> ctx,
      byte[] splitKey) throws IOException;

  /**
   * This will be called after PONR step as part of split transaction Calling
   * {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no effect in this
   * hook.
   * @param ctx
   * @throws IOException
   */
  void preSplitAfterPONR(final ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * This is used to roll back the split related transactions.
   * @param ctx
   * @return
   * @throws IOException
   */
  void preRollBack(final ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException;

  /**
   * Used after closeRegionOperation in the batchMutate()
   * @param ctx
   * @throws IOException
   */
  void postCloseRegionOperation(final ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException;

  /**
   * Used after startRegionOperation in the batchMutate()
   * @param ctx
   * @throws IOException
   */
  void postStartRegionOperation(final ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException;

}
