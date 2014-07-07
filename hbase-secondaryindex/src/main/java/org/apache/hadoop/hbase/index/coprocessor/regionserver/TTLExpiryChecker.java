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

import org.apache.hadoop.hbase.HConstants;

public class TTLExpiryChecker {
  
  public boolean checkIfTTLExpired(long ttl, long timestamp) {
    if (ttl == HConstants.FOREVER) {
      return false;
    } else if (ttl == -1) {
      return false;
    } else {
      // second -> ms adjust for user data
      ttl *= 1000;
    }
    if ((System.currentTimeMillis() - timestamp) > ttl) {
      return true;
    }
    return false;
  }
}
