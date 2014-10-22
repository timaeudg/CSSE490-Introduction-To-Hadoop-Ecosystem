CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};
CREATE TABLE IF NOT EXISTS ${hiveconf:tableName} (
year int,
temp int
)
Partitioned by (quality int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '${hiveconf:inputLocation}' INTO TABLE ${hiveconf:tableName} Partition(quality = 0, quality = 1);

SELECT MAX(temp)
FROM ${hiveconf:tableName};

SELECT MIN(temp)
FROM ${hiveconf:tableName};

SELECT AVG(temp)
FROM ${hiveconf:tableName};