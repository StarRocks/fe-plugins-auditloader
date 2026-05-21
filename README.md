# `fe-plugins-auditloader`

This document describes how to manage audit information within StarRocks using the AuditLoader plugin.

### Plugin description:

In StarRocks, all audit information is only stored in the log file **fe/log/fe.audit.log** and cannot be accessed directly through StarRocks. The AuditLoader plugin enables audit information to be loaded into the database, allowing you to conveniently view and manage cluster audit information via SQL within StarRocks. After installing the AuditLoader plugin, StarRocks will automatically invoke the AuditLoader plugin after executing SQL to collect audit information, batch it in memory, and then load it into a StarRocks table via Stream Load.

**Note**: The number of audit log fields differs across major versions of StarRocks. To ensure version compatibility, the new audit plugin selects common log fields across major versions for database ingestion. If more complete fields are needed for your business, you can replace `fe-plugins-auditloader\lib\starrocks-fe.jar` in the project, modify the field-related content in the code, and then recompile and repackage.

### Usage instructions:

##### 1. Create an internal table

First, create a dynamic partition table in StarRocks to store the data from the audit logs. For standardized usage, it is recommended to create a dedicated database for it.

For example, create a database `starrocks_audit_db__` to store audit logs:

```SQL
create database starrocks_audit_db__;
```

Create the `starrocks_audit_tbl__` table in the `starrocks_audit_db__` database. The field order and attribute section of the table can be modified according to actual business needs:

```SQL
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId` VARCHAR(64) COMMENT "Unique ID of the query",
  `timestamp` DATETIME NOT NULL COMMENT "Query start time",
  `queryType` VARCHAR(12) COMMENT "Query type (query, slow_query, connection)",
  `clientIp` VARCHAR(32) COMMENT "Client IP",
  `user` VARCHAR(64) COMMENT "Query username",
  `authorizedUser` VARCHAR(64) COMMENT "Unique user identifier, i.e., user_identity",
  `resourceGroup` VARCHAR(64) COMMENT "Resource group name",
  `catalog` VARCHAR(32) COMMENT "Data catalog name",
  `db` VARCHAR(96) COMMENT "Database where the query is located",
  `state` VARCHAR(8) COMMENT "Query Status (EOF, ERR, OK)",
  `errorCode` VARCHAR(512) COMMENT "Error Code",
  `queryTime` BIGINT COMMENT "Query Execution Time (milliseconds)",
  `scanBytes` BIGINT COMMENT "Number of bytes scanned by the query",
  `scanRows` BIGINT COMMENT "Number of rows scanned by the query",
  `returnRows` BIGINT COMMENT "Number of rows returned by the query",
  `cpuCostNs` BIGINT COMMENT "Query CPU Time (nanoseconds)",
  `memCostBytes` BIGINT COMMENT "Query Memory Consumption (bytes)",
  `stmtId` INT COMMENT "SQL Statement Increment ID",
  `isQuery` TINYINT COMMENT "Whether the SQL is a query (1 or 0)",
  `feIp` VARCHAR(128) COMMENT "FE IP that executed the statement",
  `stmt` VARCHAR(1048576) COMMENT "Original SQL Statement",
  `digest` VARCHAR(32) COMMENT "Slow SQL Fingerprint",
  `planCpuCosts` DOUBLE COMMENT "CPU Usage During Query Planning Phase (Nanoseconds)",
  `planMemCosts` DOUBLE COMMENT "Memory Usage During Query Planning Phase (Bytes)",
  `pendingTimeMs` BIGINT COMMENT "Time the Query Has Been Waiting in the Queue (Milliseconds)",
  `candidateMVs` varchar(65533) NULL COMMENT "List of candidate MVs",
  `hitMvs` varchar(65533) NULL COMMENT "List of hit MVs",
  `QueriedRelations` ARRAY<VARCHAR(65533)> NULL COMMENT "Tables and views directly accessed by the query",
  `warehouse` VARCHAR(128) NULL COMMENT "Warehouse name"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30", --Indicates that only the audit information for the most recent 30 days is retained; this can be adjusted as needed.
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3" --If the number of BEs in the cluster is no more than 3, the replica count can be adjusted to 1; adjusting this is not recommended for production clusters.
);
```

> Note:
>
> To facilitate TTL (Time to Live) lifecycle management of audit information, it is generally recommended to create the table `starrocks_audit_tbl__` as a dynamic partition table. The table creation statement above does not explicitly create partitions, so after the table is created, you need to wait for the background dynamic partition scheduling thread to run before partitions for the current day and the next three days are generated. The dynamic partition scheduling process runs every 10 minutes by default (fe.conf dynamic\_partition\_check\_interval\_seconds). You can first check whether the partitions have been created before proceeding with subsequent operations. The command to view partitions is:
>
> ```SQL
> show partitions from starrocks_audit_db__.starrocks_audit_tbl__;
> ```

##### 2. Modify the configuration file

The audit plugin package is named `auditloader.zip`. Unzip the plugin:

```SHELL
[root@node01 ~]# unzip auditloader.zip
Archive:  auditloader.zip
  inflating: auditloader.jar
  inflating: plugin.conf
  inflating: plugin.properties
```

This command extracts the files inside the zip directly to the current directory. After extraction, you will get three files from the plugin:

- `auditloader.jar`: The compiled program jar package of the audit plugin code.
- `plugin.conf`: The plugin configuration file, used to provide configuration parameters for the underlying Stream Load writes performed by the plugin. It needs to be modified according to the cluster information. It is generally recommended to only modify the `user` and `password` information.
- `plugin.properties`: The plugin properties file, used to provide descriptive information about the audit plugin within the StarRocks cluster. No modification is needed.

Modify the configuration file `plugin.conf` according to the actual cluster information:

```XML
### plugin configuration

# The max size of a batch, default is 50MB
max_batch_size=52428800

# The max interval of batch loaded, default is 60 seconds
max_batch_interval_sec=60

# the max stmt length to be loaded in audit table, default is 1048576
max_stmt_length=1048576

# StarRocks FE host for loading the audit, default is 127.0.0.1:8030
# this should be the host port for stream load
frontend_host_port=127.0.0.1:8030

# If the response time of a query exceed this threshold, it will be recored in audit table as slow_query
qe_slow_log_ms=5000

# the capacity of audit queue, default is 1000
max_queue_size=1000

# Database of the audit table
database=starrocks_audit_db__

# Audit table name, to save the audit data
table=starrocks_audit_tbl__

# StarRocks user. This user must have import permissions for the audit table
user=root

# StarRocks user's password
password=

# StarRocks password encryption key, with a length not exceeding 16 bytes
secret_key=

# Whether to generate sql digest for all queries
enable_compute_all_query_digest=false

# Filter conditions when importing audit information
filter=
```

> Note:
> 1. It is recommended to use the default configuration of the parameter `frontend_host_port`, which is `127.0.0.1:8030`. Each FE in StarRocks independently manages its own audit information. After the audit plugin is installed, each FE will start its own background thread to collect and batch audit information and perform Stream Load writes. The `frontend_host_port` configuration item is used to provide the IP and port for the HTTP protocol for the plugin's background Stream Load tasks. This parameter does not support multiple values. The IP portion can be set to any FE's IP within the cluster, but this is not recommended, because if that FE encounters an issue, the audit information write tasks from other FEs' backgrounds will also fail due to communication failure. It is recommended to use the default `127.0.0.1:8030`, so that each FE uses its own HTTP port for communication, thereby avoiding the impact of other FE failures on communication (of course, all write tasks will ultimately be automatically forwarded to the FE Leader node for execution).
> 2. The `secret_key` parameter is used to configure the "key string for encrypting the password". In the audit plugin, its length must not exceed 16 bytes. If this parameter is left empty, it means the password in `plugin.conf` will not be encrypted or decrypted, and the plaintext password can be configured directly in the password field. If this parameter is not empty, it means the password needs to be encrypted and decrypted, and the password field should be set to the encrypted string. The encrypted password can be generated in StarRocks using the `AES_ENCRYPT` function: `SELECT TO_BASE64(AES_ENCRYPT('password','secret_key'));`.
> 3. The `enable_compute_all_query_digest` parameter indicates whether to generate a Hash SQL fingerprint for all queries (StarRocks only enables SQL fingerprinting for slow queries by default; note that the fingerprint calculation method in the plugin is inconsistent with the method inside FE — FE will[normalize](https://docs.mirrorship.cn/zh/docs/administration/Query_planning/#%E6%9F%A5%E7%9C%8B-sql-%E6%8C%87%E7%BA%B9), while the plugin will not, and if this parameter is enabled, fingerprint calculation will consume additional computing resources in the cluster).
> 4. The `filter` parameter can be used to configure filter conditions for audit information ingestion. It is implemented using the [where parameter](https://docs.mirrorship.cn/zh/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/#opt_properties)in Stream Load, i.e., `-H "where: <condition>"`. It is empty by default. Configuration example: `filter=isQuery=1 and clientIp like '127.0.0.1%' and user='root'`.
>
> After making the modifications, repackage the three files above into a zip file:
>
> ```SHELL
> [root@node01 ~]# zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
> ```
>
> **Note**: This command will move the files to be packaged into auditloader.zip and overwrite the existing auditloader.zip file in that directory. That is, after executing the packaging command, only one latest auditloader.zip plugin package file will remain in that directory.

##### 3. Distribute the plugin

Distribute auditloader.zip to all FE nodes in the cluster. The distribution path must be the same on each node. For example, distribute it to the StarRocks deployment directory `/opt/module/starrocks/`, meaning the path of the auditloader.zip file on all FE nodes in the cluster is:

```
/opt/module/starrocks/auditloader.zip
```

> **Note**: You can also distribute `auditloader.zip` to an HTTP service accessible by all FEs (such as `httpd` or `nginx`), and then install it using a network path. Note that in both cases, `auditloader.zip` must remain at that path after installation and must not be deleted after installation.

##### 4. Install the plugin

The syntax for installing a local plugin in StarRocks is:

```sql
INSTALL PLUGIN FROM "/location/plugindemo.zip";
```

If installing via a network path, you also need to provide the md5 information of the plugin archive in the installation command's properties. Syntax example:

```sql
INSTALL PLUGIN FROM "http://192.168.141.203/extra/auditloader.zip" PROPERTIES("md5sum" = "3975F7B880C9490FE95F42E2B2A28E2D");
```

Taking the installation of a local plugin package as an example, modify the command according to the path of the distributed file described above and then execute it:

```SQL
mysql> INSTALL PLUGIN FROM "/opt/module/starrocks/auditloader.zip";
```

After installation is complete, view the currently installed plugin information:

```SQL
mysql> SHOW PLUGINS\G
*************************** 1. row ***************************
       Name: __builtin_AuditLogBuilder
       Type: AUDIT
Description: builtin audit logger
    Version: 0.12.0
JavaVersion: 1.8.31
  ClassName: com.starrocks.qe.AuditLogBuilder
     SoName: NULL
    Sources: Builtin
     Status: INSTALLED
 Properties: {}
*************************** 2. row ***************************
       Name: AuditLoader
       Type: AUDIT
Description: Available for versions 2.5+. Load audit log to starrocks, and user can view the statistic of queries
    Version: 4.2.1
JavaVersion: 1.8.0
  ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
     SoName: NULL
    Sources: /opt/module/starrocks/auditloader.zip
     Status: INSTALLED
 Properties: {}
```

You can see that the current cluster has two plugins. The plugin with Name `AuditLoader` is the audit log plugin installed above, and a plugin status of INSTALLED indicates successful installation. The plugin with Name `__builtin_AuditLogBuilder` is the built-in audit plugin of StarRocks, used to print audit information to the local log directory to generate fe.audit.log. Both plugins source their data from the same FE method, and under normal circumstances the data in the audit log table should remain consistent with the content in the local audit log file.

##### 5. Uninstall the Plugin

When you need to upgrade the plugin or adjust its configuration, the already-installed AuditLoader plugin can also be uninstalled as needed. The syntax for the uninstall command is:

```SQL
UNINSTALL PLUGIN plugin_name;
--plugin_name is the plugin Name information retrieved by the SHOW PLUGINS command, and it should typically be AuditLoader.
```

> **Note**:
>
> `fe/plugins` is the installation directory for StarRocks external plugins. After the audit plugin is installed, an AuditLoader folder will be created in the `fe/plugins` directory of each FE (this directory is automatically deleted after the plugin is uninstalled). If you need to modify the plugin configuration later, in addition to uninstalling and reinstalling the plugin (recommended), you can also replace the auditloader.jar in that directory or modify `plugin.conf`, then restart FE for the changes to take effect.

##### 6. View Data

After the AuditLoader plugin is installed, audit information from SQL executions is not loaded into the database in real time. The StarRocks backend will batch data for 60 seconds or 50M according to the parameters configured in the configuration file plugin.conf before executing a Stream Load import. During the test waiting period, you can simply execute a few SQL statements to check whether the corresponding audit data can be loaded into the database normally, for example:

```sql
mysql> CREATE DATABASE test;
mysql> CREATE TABLE test.audit_test(c1 int,c2 int,c3 int,c4 date,c5 bigint) Distributed BY hash(c1) properties("replication_num" = "1");
mysql> insert into test.audit_test values(211014001,10001,13,'2021-10-14',1999),(211014002,10002,13,'2021-10-14',6999),(211015098,16573,19,'2021-10-15',3999);
```

Wait about 1 minute, then view the audit table data:

```sql
mysql> select * from starrocks_audit_db__.starrocks_audit_tbl__;
```

##### 7. Troubleshooting

Under normal circumstances, data can be correctly loaded into the database. If the plugin is installed successfully and dynamic partitions are created successfully but audit information has still not been imported into the table for a long time, you can check whether the IP, port, user permissions, user password, and other information in the configuration file plugin.conf are correct. The AuditLoader logs are printed in the `fe.log` of each FE. You can also search for the keyword `audit` in `fe.log` and use the approach for troubleshooting Stream Load tasks to locate the issue.

##### 8. Extended Usage

The `queryType` types supported in the StarRocks audit table include: query, slow\_query, and connection. For query and slow\_query, the AuditLoader plugin uses the `qe_slow_log_ms` time configured in `plugin.conf` for comparison and judgment. SQL statements with an execution time greater than `qe_slow_log_ms` are classified as slow\_query, which you can use to perform statistics on slow SQL in the cluster.

For connection, StarRocks 3.0.6+ supports printing successful/failed connection information when a client connects in `fe.audit.log`. You can configure `audit_log_modules=slow_query,query,connection` in `fe.conf` and then restart FE to enable it. After enabling connection information, the AuditLoader plugin can also collect this type of client connection information and load it into the table `starrocks_audit_tbl__`. After loading, the `queryType` field of the audit table for this type of information will be connection, which you can use to audit user login information.

### Release Notes:

##### AuditLoader  v4.2.1

1. Added the ability to configure encrypted passwords in plugin.conf

2. Added two important monitoring fields, candidateMVs and hitMvs, as reserved fields in the audit log table

3. Adapted to the StarRocks audit log QueriedRelations field

4. Added the ability to filter audit information storage conditions via the filter parameter in plugin.conf

5. Adjusted the plugin batching logic to Json, to avoid the issue where upgrading the FE `netty` dependency in StarRocks 3.2.12+ and other versions caused the original CSV batching logic to report the error `Validation failed for header 'column_separator'` during writes

6. Other minor optimizations
