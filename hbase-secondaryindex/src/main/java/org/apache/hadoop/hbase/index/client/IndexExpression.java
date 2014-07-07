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

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.Constants;

/**
 * While scan the index(s) to be used can be explicitly passed from client application.
 * Objects of IndexExpression can specify the index(s) details.
 * 
 * @see SingleIndexExpression
 * @see MultiIndexExpression
 * @see NoIndexExpression
 * @see Scan#setAttribute(String, byte[])
 * @see Constants#INDEX_EXPRESSION
 * @see IndexUtils#toBytes(IndexExpression)
 */
public interface IndexExpression extends Serializable {

}
