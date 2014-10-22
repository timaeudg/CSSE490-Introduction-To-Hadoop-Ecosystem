CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};
CREATE TABLE IF NOT EXISTS ${hiveconf:tableName} (
text string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '${hiveconf:inputLocation}' INTO TABLE ${hiveconf:tableName};

DROP FUNCTION IF EXISTS Strip;
CREATE FUNCTION Strip AS 'Task2.Strip' USING JAR 'hdfs:///user/root/Lab6Hive-0.0.1-SNAPSHOT.jar';
DROP FUNCTION IF EXISTS Upper;
CREATE FUNCTION Upper AS 'Task2.ToUpper' USING JAR 'hdfs:///user/root/Lab6Hive-0.0.1-SNAPSHOT.jar;

SELECT Upper(Strip(word)), COUNT(*) 
FROM ${hiveconf:tableName}
LATERAL VIEW explode(split(text, ' ')) lTable as word GROUP BY Upper(Strip(word));