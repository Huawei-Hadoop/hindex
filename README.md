hindex - Secondary Index for HBase
======

The solution is 100% Java, compatible with Apache HBase 0.94.8, and is open sourced under ASL.

Following capabilities are supported currently.
- multiple indexes on table,
- multi column index,
- index based on part of a column value,
- equals and range condition scans using index, and
- bulk loading data to indexed table (Indexing done with bulk load).


## How it works
HBase Secondary Index is 100% server side implementation with co processors which persists index data in a separate table. Indexing is region wise and custom load balancer co-locates the index table regions with actual table regions. 

![si1](https://f.cloud.github.com/assets/5187670/945574/6a0f3bc0-0316-11e3-845c-de653152923a.jpg)

Server reads the Index specification passed during the table creation and creates the index table. There will be one index table for one user table and all index information for that user table goes into the same index table.

## Put Operation
When a row is put into the HBase (user) table, co processors prepare and put the index information  in the corresponding index table.
Index table rowkey = region startkey + index name + indexed column value + user table rowkey

E.g.:  

Table –> tab1 column family –> cf

Index –> idx1, cf1:c1 and idx2, cf1:c2

Index table –> tab1_idx (user table name with suffix “_idx” )

![si2](https://f.cloud.github.com/assets/5187670/945582/a6140d1c-0316-11e3-9af2-12c9fa636441.jpg)

## Scan Operation 
For a user table scan, co processor creates a scanner on the index table, scans the index data and seeks to exact rows in the user table. These seeks on HFiles are based on rowkey obtained from index data. This will help to skip the blocks where data is not present and sometimes full HFiles may also be skipped. 

![si5](https://f.cloud.github.com/assets/5187670/945631/32f0fc9e-0318-11e3-9a44-d2c7496f1d64.jpg)

![si4](https://f.cloud.github.com/assets/5187670/945610/803d9aee-0317-11e3-8827-5cbc60e6efbb.jpg)


## Usage
Clients need to pass the IndexedHTableDescriptor with the index name and columns while creating the table 

    IndexedHTableDescriptor htd = new IndexedHTableDescriptor(usertableName);

    IndexSpecification iSpec = new IndexSpecification(indexName);
    
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    
    iSpec.addIndexColumn(hcd, indexColumnQualifier, ValueType.String, 10);
    
    htd.addFamily(hcd);
    
    htd.addIndex(iSpec);
    
    admin.createTable(htd);
    
No changes required for Puts, Deletes at client side as index operations for the same are internally handled by co-processors

No change in scan code for the client app. 

No need to specify the index(s) to be used. Secondary Index implementation finds the best index for Scan by analyzing the filters used for the query.

## Source 
This repository contains source for Secondary Index support on Apache HBase 0.94.8.

## Building from source and testing
Building from source procedure is same as building HBase source hence it requires
- Java 1.6 or later
- Maven 3.X

Separate test source (secondaryindex\src\test\java\ )is available for running the tests on secondary indexes.

## Note
Configure following configurations in hbase-site.xml for using secondary index.

**Property**
- **name** - *hbase.use.secondary.index*
- __value__ -  *true*
- __description__ - *Enable this property when you are using secondary index*

__Property__
- __name__ - *hbase.coprocessor.master.classes*
- __value__ -  *org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver*
- __description__ - *A comma-separated list of org.apache.hadoop.hbase.coprocessor.MasterObserver coprocessors that are loaded by default on the active HMaster process. For any implemented coprocessor methods, the listed classes will be called in order. After implementing your own MasterObserver, just put it in HBase's classpath and add the fully qualified class name here. 
org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver -defines of coprocessor hooks to support secondary index operations on master process.*

__Property__
- __name__ - *hbase.coprocessor.region.classes*
- __value__ -  *org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver*
- __description__ - *A comma-separated list of Coprocessors that are loaded by default on all tables. For any override coprocessor method, these classes will be called in order. After implementing your own Coprocessor, just put it in HBase's classpath and add the fully qualified class name here. A coprocessor can also be loaded on demand by setting HTableDescriptor.
org.apache.hadoop.hbase.index.coprocessor.regionserver.IndexRegionObserver –class defines coprocessor hooks to support secondary index operations on Region.*

__Property__
- __name__ - *hbase.coprocessor.wal.classes*
- __value__ -  *org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver*
- __description__ - *Classes which defines coprocessor hooks to support WAL operations.
org.apache.hadoop.hbase.index.coprocessor.wal.IndexWALObserver – class define coprocessors hooks to support secondary index WAL operations*

## Future Work
- Dynamically add/drop index
- Integrate Secondary Index Management in the HBase Shell 
- Optimize range scan scenarios
- HBCK tool support for Secondary index tables
- WAL Optimizations for Secondary index table entries
- Make Scan Evaluation Intelligence Pluggable
