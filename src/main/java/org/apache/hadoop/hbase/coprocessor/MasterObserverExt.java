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

/**
 * This is an extension for the MasterObserver interface. The APIs added into this interface are not
 * exposed by HBase. This is internally being used by CMWH HBase. Customer should not make use of
 * this interface points. <br>
 * Note : The APIs in this interface is subject to change at any time.
 */
public interface MasterObserverExt {

  /**
   * Call before the master initialization is set to true.
   */
  void preMasterInitialization(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException;
}
