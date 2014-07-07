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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndexImportTsv extends ImportTsv {

  final static String NAME = "indeximporttsv";
  final static Class DEFAULT_MAPPER = IndexTsvImporterMapper.class;

  /**
   * Sets up the actual job.
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   * @throws InterruptedException
   */
  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException,
      ClassNotFoundException {
    HBaseAdmin admin = new IndexAdmin(conf);
    // Support non-XML supported characters
    // by re-encoding the passed separator as a Base64 string.
    String actualSeparator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
    if (actualSeparator != null) {
      conf.set(ImportTsv.SEPARATOR_CONF_KEY, Base64.encodeBytes(actualSeparator.getBytes()));
    }

    // See if a non-default Mapper was set
    String mapperClassName = conf.get(ImportTsv.MAPPER_CONF_KEY);
    Class mapperClass = mapperClassName != null ? Class.forName(mapperClassName) : DEFAULT_MAPPER;

    String tableName = args[0];
    Path inputDir = new Path(args[1]);

    String input = conf.get(IndexUtils.TABLE_INPUT_COLS);
    HTableDescriptor htd = null;
    if (!admin.tableExists(tableName)) {
      htd =
          ImportTsv.prepareHTableDescriptor(tableName, conf.getStrings(ImportTsv.COLUMNS_CONF_KEY));
      if (input != null) {
        htd = IndexUtils.parse(tableName, htd, input, null);
      }
      admin.createTable(htd);
    }

    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(IndexMapReduceUtil.IS_INDEXED_TABLE, input != null);
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(mapperClass);
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(mapperClass);

    String hfileOutPath = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
    if (hfileOutPath != null) {
      HTable table = new HTable(conf, tableName);
      job.setReducerClass(PutSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(Put.class);
      IndexHFileOutputFormat.configureIncrementalLoad(job, table);
    } else {
      // No reducers. Just write straight to table. Call initTableReducerJob
      // to set up the TableOutputFormat.
      TableMapReduceUtil.initTableReducerJob(tableName, null, job);
      job.setNumReduceTasks(0);
    }

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
      com.google.common.base.Function.class /* Guava used by TsvParser */);
    return job;
  }

  /*
   * @param errorMsg Error message. Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String usage =
        "Usage: "
            + NAME
            + " -Dimporttsv.columns=a,b,c <tablename> <inputdir>\n"
            + "\n"
            + "Imports the given input directory of TSV data into the specified table.\n"
            + "\n"
            + "The column names of the TSV data must be specified using the -Dimporttsv.columns\n"
            + "option. This option takes the form of comma-separated column names, where each\n"
            + "column name is either a simple column family, or a columnfamily:qualifier. The special\n"
            + "column name HBASE_ROW_KEY is used to designate that this column should be used\n"
            + "as the row key for each imported record. You must specify exactly one column\n"
            + "to be the row key, and you must specify a column name for every column that exists in the\n"
            + "input data. Another special column HBASE_TS_KEY designates that this column should be\n"
            + "used as timestamp for each record. Unlike HBASE_ROW_KEY, HBASE_TS_KEY is optional.\n"
            + "You must specify atmost one column as timestamp key for each imported record.\n"
            + "Record with invalid timestamps (blank, non-numeric) will be treated as bad record.\n"
            + "Note: if you use this option, then 'importtsv.timestamp' option will be ignored.\n"
            + "\n"
            + "By default importtsv will load data directly into HBase. To instead generate\n"
            + "HFiles of data to prepare for a bulk data load, pass the option:\n"
            + "  -D"
            + ImportTsv.BULK_OUTPUT_CONF_KEY
            + "=/path/for/output\n"
            + "  Note: if you do not use this option, then the target table must already exist in HBase\n"
            + "\n"
            + "Other options that may be specified with -D include:\n"
            + "  -D"
            + ImportTsv.SKIP_LINES_CONF_KEY
            + "=false - fail if encountering an invalid line\n"
            + "  '-D"
            + ImportTsv.SEPARATOR_CONF_KEY
            + "=|' - eg separate on pipes instead of tabs\n"
            + "  -D"
            + ImportTsv.TIMESTAMP_CONF_KEY
            + "=currentTimeAsLong - use the specified timestamp for the import\n"
            + "  -D"
            + ImportTsv.MAPPER_CONF_KEY
            + "=my.Mapper - A user-defined Mapper to use instead of "
            + DEFAULT_MAPPER.getName()
            + "\n"
            + "For performance consider the following options:\n"
            + "  -Dmapred.map.tasks.speculative.execution=false\n"
            + "  -Dmapred.reduce.tasks.speculative.execution=false\n"
            + "  -Dtable.columns.index='IDX1=>cf1:[q1->datatype& length],[q2],"
            + "[q3];cf2:[q1->datatype&length],[q2->datatype&length],[q3->datatype& lenght]#IDX2=>cf1:q5,q5'"
            + "     The format used here is: \n" + "     IDX1 - Index name\n"
            + "     cf1 - Columnfamilyname\n" + "     q1 - qualifier\n"
            + "     datatype - datatype (Int, String, Double, Float)\n"
            + "     length - length of the value\n"
            + "     The columnfamily should be seperated by ';'\n"
            + "     The qualifier and the datatype and its length should be enclosed in '[]'.\n"
            + "     The qualifier details are specified using '->' following qualifer name and the"
            + " details are seperated by '&'\n"
            + "     If the qualifier details are not specified default values are used.\n"
            + "     # is used to seperate between two index details";
    System.err.println(usage);
  }

  /**
   * Main entry point.
   * @param args The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }

    // Make sure columns are specified
    String columns[] = conf.getStrings(ImportTsv.COLUMNS_CONF_KEY);
    if (columns == null) {
      usage("No columns specified. Please specify with -D" + ImportTsv.COLUMNS_CONF_KEY + "=...");
      System.exit(-1);
    }

    // Make sure they specify exactly one column as the row key
    int rowkeysFound = 0;
    for (String col : columns) {
      if (col.equals(TsvParser.ROWKEY_COLUMN_SPEC)) rowkeysFound++;
    }
    if (rowkeysFound != 1) {
      usage("Must specify exactly one column as " + TsvParser.ROWKEY_COLUMN_SPEC);
      System.exit(-1);
    }

    // Make sure we have at most one column as the timestamp key
    int tskeysFound = 0;
    for (String col : columns) {
      if (col.equals(TsvParser.TIMESTAMPKEY_COLUMN_SPEC)) tskeysFound++;
    }
    if (tskeysFound > 1) {
      usage("Must specify at most one column as " + TsvParser.TIMESTAMPKEY_COLUMN_SPEC);
      System.exit(-1);
    }

    // Make sure one or more columns are specified excluding rowkey and timestamp key
    if (columns.length - (rowkeysFound + tskeysFound) < 1) {
      usage("One or more columns in addition to the row key and timestamp(optional) are required");
      System.exit(-1);
    }

    // If timestamp option is not specified, use current system time.
    long timstamp = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, System.currentTimeMillis());

    // Set it back to replace invalid timestamp (non-numeric) with current system time
    conf.setLong(ImportTsv.TIMESTAMP_CONF_KEY, timstamp);

    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
