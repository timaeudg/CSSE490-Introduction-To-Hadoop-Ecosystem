USE ${hiveconf:databaseName};

LOAD DATA INPATH '${hiveconf:pigOutputLocation}' INTO TABLE ${hiveconf:tempTableName} Partition(username='timaeudg');

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${hiveconf:tableName} PARTITION(username) 
SELECT name,cno,cname,grade,username from ${hiveconf:tempTableName};