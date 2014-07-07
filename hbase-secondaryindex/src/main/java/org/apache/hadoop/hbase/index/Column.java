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
package org.apache.hadoop.hbase.index;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

public class Column implements Serializable {
  private static final long serialVersionUID = -1958705310924323448L;

  private byte[] cf;
  private byte[] qualifier;
  private ValuePartition valuePartition = null;

  public Column() {
  }

  public Column(byte[] cf, byte[] qualifier) {
    this.cf = cf;
    this.qualifier = qualifier;
  }

  public Column(byte[] cf, byte[] qualifier, ValuePartition vp) {
    this.cf = cf;
    this.qualifier = qualifier;
    this.valuePartition = vp;
  }

  public void setFamily(byte[] cf) {
    this.cf = cf;
  }

  public void setQualifier(byte[] qualifier) {
    this.qualifier = qualifier;
  }

  public byte[] getFamily() {
    return cf;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public ValuePartition getValuePartition() {
    return this.valuePartition;
  }

  public void setValuePartition(ValuePartition vp) {
    this.valuePartition = vp;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof Column)) return false;
    Column that = (Column) obj;
    if (!(Bytes.equals(this.cf, that.cf))) return false;
    if (!(Bytes.equals(this.qualifier, that.qualifier))) return false;
    if (valuePartition == null && that.valuePartition == null) {
      return true;
    } else if (valuePartition != null && that.valuePartition != null) {
      return valuePartition.equals(that.valuePartition);
    } else {
      return false;
    }
  }

  public int hashCode() {
    int result = Bytes.hashCode(this.cf);
    result ^= Bytes.hashCode(this.qualifier);
    if (valuePartition != null) result ^= valuePartition.hashCode();
    return result;
  }

  public String toString() {
    return Bytes.toString(this.cf) + " : " + Bytes.toString(this.qualifier);
  }
}
