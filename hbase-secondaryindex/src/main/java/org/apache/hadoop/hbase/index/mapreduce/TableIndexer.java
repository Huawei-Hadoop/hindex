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
package org.apache.hadoop.hbase.index.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TableIndexer {

  public static final String TABLE_NAME_TO_INDEX = "tablename.to.index";
  final static String BULK_OUTPUT_CONF_KEY = "import.bulk.output";

  private final static int DEFAULT_CACHING = 500;
  private final static int DEFAULT_VERSIONS = 1;

  // This can be a comma seperated list
  // We can pass like
  // IDX1=>cf1:[q1->datatype&
  // length],[q2],[q3];cf2:[q1->datatype&length],[q2->datatype&length],[q3->datatype&
  // lenght]#IDX2=>cf1:q5,q5

  private static Map<String, List<String>> cfs = new HashMap<String, List<String>>();

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int caching = -1;
    int versions = -1;
    if (otherArgs.length < 2) {
      System.out
          .println("Caching and/or Versions  not specified properly, using default values [default caching: "
              + DEFAULT_CACHING + ", default versions: " + DEFAULT_VERSIONS + "]");
      try {
        caching = Integer.parseInt(otherArgs[0]);
      } catch (NumberFormatException nfe) {
        caching = DEFAULT_CACHING;
      }
      try {
        versions = Integer.parseInt(otherArgs[1]);
      } catch (NumberFormatException nfe) {
        versions = DEFAULT_VERSIONS;
      }
    }

    String tableNameToIndex = conf.get(TABLE_NAME_TO_INDEX);
    if (tableNameToIndex == null) {
      System.out
          .println("Wrong usage.  Usage is pass the table -Dtablename.to.index=table1 "
              + "-Dtable.columns.index='IDX1=>cf1:[q1->datatype& length],[q2],"
              + "[q3];cf2:[q1->datatype&length],[q2->datatype&length],[q3->datatype& lenght]#IDX2=>cf1:q5,q5'");
      System.out.println("The format used here is: ");
      System.out.println("IDX1 - Index name");
      System.out.println("cf1 - Columnfamilyname");
      System.out.println("q1 - qualifier");
      System.out.println("datatype - datatype (Int, String, Double, Float)");
      System.out.println("length - length of the value");
      System.out.println("The columnfamily should be seperated by ';'");
      System.out
          .println("The qualifier and the datatype and its length should be enclosed in '[]'."
              + "  The qualifier details are specified using '->' following qualifer name and the details are seperated by '&'");
      System.out.println("If the qualifier details are not specified default values are used.");
      System.out.println("# is used to seperate between two index details");
      System.out.println("Pass the scanner caching and maxversions as arguments.");
      System.exit(-1);
    }
    IndexUtils.createIndexTable(tableNameToIndex, conf, cfs);
    createMapReduceJob(tableNameToIndex, conf, caching, versions);
  }

  private static void createMapReduceJob(String tableNameToIndex, Configuration conf, int caching,
      int versions) throws IOException, InterruptedException, ClassNotFoundException {
    // Set the details to TableInputFormat
    Scan s = new Scan();
    s.setCaching(caching);
    s.setMaxVersions(versions);
    conf.set(TableInputFormat.INPUT_TABLE, tableNameToIndex);

    Set<Entry<String, List<String>>> entrySet = cfs.entrySet();
    for (Entry<String, List<String>> entry : entrySet) {
      List<String> quals = entry.getValue();
      addColumn(quals, Bytes.toBytes(entry.getKey()), s);
    }
    Job job = new Job(conf, "CreateIndex");
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    TableMapReduceUtil.initTableMapperJob(tableNameToIndex, // input table
      s, // Scan instance to control CF and attribute selection
      IndexCreationMapper.class, // mapper class
      ImmutableBytesWritable.class, // mapper output key
      Put.class, // mapper output value
      job);

    TableMapReduceUtil.initTableReducerJob(IndexUtils.getIndexTableName(tableNameToIndex), // output
                                                                                           // table
      null, // reducer class
      job);

    if (hfileOutPath != null) {
      HTable table = new HTable(conf, tableNameToIndex);
      job.setReducerClass(KeyValueSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      HFileOutputFormat.configureIncrementalLoad(job, table);
    } else {
      job.setNumReduceTasks(0);
    }
    job.setJarByClass(IndexCreationMapper.class);
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
      com.google.common.base.Function.class /* Guava used by TsvParser */);

    job.waitForCompletion(true);
    assert job.isComplete() == true;
  }

  private static void addColumn(List<String> quals, byte[] cf, Scan s) {
    for (String q : quals) {
      s.addColumn(cf, Bytes.toBytes(q));
    }
  }

}
