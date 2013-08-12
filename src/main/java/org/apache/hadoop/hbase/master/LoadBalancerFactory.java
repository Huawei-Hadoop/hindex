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

package org.apache.hadoop.hbase.master;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The class that creates a load balancer from a conf.
 */
public class LoadBalancerFactory {

  private static final Log LOG = LogFactory.getLog(LoadBalancerFactory.class);

  static Class<?> secIndexLoadBalancerKlass = LoadBalancer.class;

  /**
   * Create a loadblanacer from the given conf.
   * @param conf
   * @return A {@link LoadBalancer}
   */
  public static LoadBalancer getLoadBalancer(Configuration conf) {
    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass =
        conf.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, DefaultLoadBalancer.class,
          LoadBalancer.class);
    boolean secondaryIndex = conf.getBoolean("hbase.use.secondary.index", false);
    String secIndexBalancer = conf.get("hbase.index.loadbalancer.class");
    if (secondaryIndex) {
      if (secIndexBalancer == null) {
        throw new RuntimeException(
            "Secondary index load  balancer not configured. Configure the property hbase.index.loadbalancer.class");
      }
      try {
        secIndexLoadBalancerKlass = Class.forName(secIndexBalancer.trim());
        Object secIndexLoadBalancerInstance = secIndexLoadBalancerKlass.newInstance();
        Method method = secIndexLoadBalancerKlass.getMethod("setDelegator", LoadBalancer.class);
        method.invoke(secIndexLoadBalancerInstance,
          (LoadBalancer) ReflectionUtils.newInstance(balancerKlass, conf));
        return (LoadBalancer) secIndexLoadBalancerInstance;
      } catch (Throwable t) {
        LOG.error("Error while initializing/invoking method of seconday index load balancer.", t);
        throw new RuntimeException("Not able to load the secondary index load balancer class.");
      }

    }
    return ReflectionUtils.newInstance(balancerKlass, conf);
  }
}
