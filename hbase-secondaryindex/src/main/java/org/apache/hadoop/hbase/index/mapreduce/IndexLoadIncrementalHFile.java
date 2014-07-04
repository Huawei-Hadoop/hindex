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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.index.util.IndexUtils;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.util.ToolRunner;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * 
 * @see #usage()
 */
public class IndexLoadIncrementalHFile extends LoadIncrementalHFiles {

  public final static String NAME = "indexcompletebulkload";

  public IndexLoadIncrementalHFile(Configuration conf) throws Exception {
    super(conf);
    familyDirFilter = new PathFilter() {
      public boolean accept(Path p) {
        return !p.getName().startsWith("_")
            && !p.getName().startsWith(IndexMapReduceUtil.INDEX_DATA_DIR);
      }
    };
  }

  private void usage() {
    System.err.println("usage: " + NAME + " /path/to/hfileoutputformat-output " + "tablename");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return -1;
    }

    String dirPath = args[0];
    TableName tableName = TableName.valueOf(args[1]);

    boolean tableExists = this.doesTableExist(tableName);
    if (!tableExists) this.createTable(tableName, dirPath);

    String indexTableName = IndexUtils.getIndexTableName(tableName);
    boolean indexedTable = this.hbAdmin.isTableAvailable(TableName.valueOf(indexTableName));
    if (indexedTable) {
      // load the index data to the indextable
      Path indxhfDir = new Path(dirPath, IndexMapReduceUtil.INDEX_DATA_DIR);
      HTable idxTable = new HTable(getConf(), indexTableName);
      doBulkLoad(indxhfDir, idxTable);
    }

    Path hfofDir = new Path(dirPath);
    HTable table = new HTable(getConf(), tableName);
    doBulkLoad(hfofDir, table);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IndexLoadIncrementalHFile(HBaseConfiguration.create()), args);
    System.exit(ret);
  }
}
