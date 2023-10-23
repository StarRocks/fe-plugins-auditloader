# fe-plugins-auditloader
### Description：

In StarRocks, all SQL audit logs are saved in a local file named fe/log/fe.audit.log, and are not stored in the database. To facilitate the analysis of business SQL, a plugin has been developed to load audit information into StarRocks and allow users to view query statistics. The plugin is implemented such that StarRocks calls the plugin to collect audit information after executing an SQL statement. The audit information is collected in memory and then imported into StarRocks tables based on the Stream Load method.

**Important Notes:**

1、Using the plugin: As StarRocks iterates and upgrades, the number of fields in the audit log file fe.audit.log may gradually increase. Therefore, different StarRocks versions require corresponding versions of the plugin, and the creation statement of the audit log table in StarRocks must also be adjusted. The current code and the following demonstration use the creation statement of the audit log table suitable for StarRocks 2.4 and later versions.

2、Developing the plugin: If you find that the audit log fields or format in the new version of StarRocks have changed, you need to replace the fe-plugins-auditloader\lib\starrocks-fe.jar with the new version of StarRocks package's fe/lib/starrocks-fe.jar and modify the code related to fields.


### Usage Instructions:

##### 1、Create an internal table

First, create a dynamic partition table in StarRocks to save the audit log data. To standardize usage, it is recommended to create a separate database for it.

For example, create a database named starrocks_audit_db__:

```SQL
create database starrocks_audit_db__;
```

Create the starrocks_audit_tbl__ table in the starrocks_audit_db__ database:

```SQL
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId` VARCHAR(48) COMMENT "Unique ID of the query",
  `timestamp` DATETIME NOT NULL COMMENT "Start time of the query",
  `queryType` VARCHAR(12) COMMENT "Query type (query, slow_query)",
  `clientIp` VARCHAR(32) COMMENT "Client IP",
  `user` VARCHAR(64) COMMENT "Query username",
  `authorizedUser` VARCHAR(64) COMMENT "Unique identifier for the user",
  `resourceGroup` VARCHAR(64) COMMENT "Resource group name",
  `catalog` VARCHAR(32) COMMENT "Data directory name",
  `db` VARCHAR(96) COMMENT "Query 所在的数据库",
  `state` VARCHAR(8) COMMENT "Query state (EOF, ERR, OK)",
  `errorCode` VARCHAR(96) COMMENT "Error code",
  `queryTime` BIGINT COMMENT "Query execution time (milliseconds)",
  `scanBytes` BIGINT COMMENT "Query scan bytes",
  `scanRows` BIGINT COMMENT "Query scan row count",
  `returnRows` BIGINT COMMENT "Query return row count",
  `cpuCostNs` BIGINT COMMENT "Query CPU cost (nanoseconds)",
  `memCostBytes` BIGINT COMMENT "Query memory cost (bytes)",
  `stmtId` INT COMMENT "SQL statement increment ID",
  `isQuery` TINYINT COMMENT "Is the SQL a query (1 or 0)",
  `feIp` VARCHAR(32) COMMENT "IP of the FE that executed the query",
  `stmt` STRING COMMENT "Original SQL statement",
  `digest` VARCHAR(32) COMMENT "Slow SQL fingerprint",
  `planCpuCosts` DOUBLE COMMENT "Query planning phase CPU cost (nanoseconds)",
  `planMemCosts` DOUBLE COMMENT "Query planning phase memory cost (bytes)"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3 
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",  --This indicates that only the audit information within the last 30 days will be retained, and the requirement can be adjusted as needed.
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"  --If the number of BEs in the cluster is no more than 3, you can adjust the number of replicas to 1. It is not recommended to adjust the production cluster.
);
```

`starrocks_audit_tbl__` is a dynamically partitioned table. Since we did not directly create partitions when building the table, we need to wait for the background dynamic partition scheduling thread to schedule before the partitions for the current day and the next three days will be generated. By default, the dynamic partition thread is scheduled every 10 minutes. We can first check if the partitions for the table have been created and then proceed with the subsequent operations. The partition inspection statement is:

```SQL
show partitions from starrocks_audit_db__.starrocks_audit_tbl__;
```



##### 2、Modify the configuration file

During the installation, the required audit plugin package is auditloader.zip. Use the unzip command to extract the plugin:

```SHELL
[root@node01 ~]# unzip auditloader.zip
Archive:  auditloader.zip
  inflating: auditloader.jar        
  inflating: plugin.conf            
  inflating: plugin.properties
```

Description: This command will extract the files inside the zip directly to the current directory. After extraction, you can get three files in the plugin:

`auditloader.jar`：The core jar package for packaging plugin code.

`plugin.conf`：The plugin configuration file, which needs to be modified according to the cluster information.

`plugin.properties`：The plugin property file, usually without modification.

According to our actual cluster information, modify the configuration file `plugin.conf`：

```XML
### plugin configuration

# The max size of a batch, default is 50MB.
max_batch_size=52428800

# The max interval of batch loaded, default is 60 seconds.
max_batch_interval_sec=60

# the max stmt length to be loaded in audit table, default is 4096.
max_stmt_length=4096

# StarRocks FE host for loading the audit, default is 127.0.0.1:8030.
# this should be the host port for stream load.
frontend_host_port=127.0.0.1:8030

# If the response time of a query exceed this threshold, it will be recored in audit table as slow_query.
qe_slow_log_ms=5000

# the capacity of audit queue, default is 1000.
max_queue_size=1000

# Database of the audit table.
database=starrocks_audit_db__

# Audit table name, to save the audit data.
table=starrocks_audit_tbl__

# StarRocks user. This user must have LOAD_PRIV to the audit table.
user=root

# StarRocks user's password.
password=
```

After modification, you can use the zip command to repackage the three files into a zip package：

```SHELL
[root@node01 ~]# zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
```

**Note: This command will move the files to be packed into auditloader.zip, and overwrite the existing auditloader.zip file in the directory.**



##### 3、Distribute the plugin

When using the local package installation method, you need to distribute auditloader.zip to all FE nodes in the cluster, and the distribution path of each node needs to be consistent. For example, we all distribute it to the StarRocks deployment directory /opt/module/starrocks/ , so the path of auditloader.zip on all FE nodes in the cluster is:

```
/opt/module/starrocks/auditloader.zip
```



##### 4、Install the plugin

The syntax for StarRocks to install a local plugin is:

```sql
INSTALL PLUGIN FROM "/location/plugin_package_name.zip";
```

For example, modify the command based on the distribution file path mentioned above and execute:

```SQL
mysql> INSTALL PLUGIN FROM "/opt/module/starrocks/auditloader.zip";
```

After installation, view the currently installed plugin information:

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
Description: Available for StarRocks 2.4 and later versions. Load audit information to StarRocks, and user can view the statistic of queries. 
    Version: 3.0.1
JavaVersion: 1.8.0
  ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
     SoName: NULL
    Sources: /opt/module/starrocks/auditloader.zip
     Status: INSTALLED
 Properties: {}
```

Currently, there are two plugins, one of which is the audit log plugin installed in the previous article, with the name AuditLoader, and its status is INSTALLED, indicating that it has been successfully installed. The plugin named __builtin_AuditLogBuilder is the built-in audit plugin of StarRocks, used to print audit information to the local log directory to generate fe.audit.log. There is no need to pay attention to it for now. It should be noted that these two plugins share the same data source. If you feel that the data in the newly installed audit plugin is not correct after being added to the database, you can compare it with fe.audit.log for verification.

**Note: fe/plugins is the installation directory of StarRocks external plugins. After the audit plugin is installed, a AuditLoader folder will be generated in the fe/plugins directory of each FE (the directory will be automatically deleted after the plugin is uninstalled). If we need to modify the plugin configuration later, in addition to uninstalling and reinstalling the plugin (recommended), we can also replace the auditloader.jar in the directory or modify the plugin.conf, and then restart the FE to make the modifications effective.**



##### 5、Uninstalling Plugins

When needed to upgrade a plugin or adjust its configuration, the AuditLoader plugin can also be uninstalled as needed. The uninstall command syntax is:

```SQL
UNINSTALL PLUGIN plugin_name;
--plugin_name refers to the Name information of the plugin found in the SHOW PLUGINS command, which should usually be AuditLoader.  
```



##### 6、Viewing Data

After the AuditLoader plugin is installed, the audit information after SQL execution is not immediately stored in the database. StarRocks will perform a Stream Load import every 60 seconds or 50M based on the configuration parameters in the plugin.conf file. During the test waiting period, you can simply execute a few SQL statements to see if the corresponding audit data can be stored normally, such as:

```sql
mysql> CREATE DATABASE test;
mysql> CREATE TABLE test.audit_test(c1 int,c2 int,c3 int,c4 date,c5 bigint) Distributed BY hash(c1) properties("replication_num" = "1");
mysql> insert into test.audit_test values(211014001,10001,13,'2021-10-14',1999),(211014002,10002,13,'2021-10-14',6999),(211015098,16573,19,'2021-10-15',3999);
```

Wait for about 1 minute and check the audit table data:

```sql
mysql> select * from starrocks_audit_db__.starrocks_audit_tbl__;
```

**Note: The data is usually stored correctly, if there is no data in the table, you can check if the IP, port, user privileges, and user password information in the configuration file plugin.conf are correct. The audit plugin's logs are printed in the fe.log of each FE, so you can also search for the keyword "audit" in fe.log and use the approach of investigating Stream Load tasks to locate the problem.**
