CREATE DATABASE IF NOT EXISTS ${hiveconf:databaseName};
USE ${hiveconf:databaseName};

CREATE TABLE IF NOT EXISTS RoseEmployees (
firstName string,
lastName string,
specialty string,
dept string,
employeeNumber int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '${hiveconf:allEmployeesLocation}' INTO TABLE RoseEmployees;

CREATE TABLE IF NOT EXISTS RoseStaticEmployees (
firstName string,
lastName string,
specialty string,
employeeNumber int
)
Partitioned by (dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '${hiveconf:csseEmployeesLocation}' INTO TABLE RoseStaticEmployees Partition(dept='csse');
LOAD DATA INPATH '${hiveconf:eceEmployeesLocation}' INTO TABLE RoseStaticEmployees Partition(dept='ece');
LOAD DATA INPATH '${hiveconf:adminEmployeesLocation}' INTO TABLE RoseStaticEmployees Partition(dept='admin');

CREATE TABLE IF NOT EXISTS RoseDynamicEmployees (
firstName string,
lastName string,
specialty string,
employeeNumber int
)
Partitioned by (dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE RoseDynamicEmployees PARTITION(dept) 
SELECT firstName,lastName,specialty,employeeNumber, dept from RoseStaticEmployees;

CREATE TABLE IF NOT EXISTS RoseStaticEmployeesORC (
firstName string,
lastName string,
specialty string,
employeeNumber int
)
Partitioned by (dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS orc;

INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='csse') 
SELECT firstName,lastName,specialty,employeeNumber 
FROM RoseEmployees
WHERE dept='csse';

INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='ece') 
SELECT firstName,lastName,specialty,employeeNumber 
FROM RoseEmployees
WHERE dept='ece';

INSERT INTO TABLE RoseStaticEmployeesORC PARTITION(dept='admin') 
SELECT firstName,lastName,specialty,employeeNumber 
FROM RoseEmployees
WHERE dept='admin';

SELECT COUNT(*)
FROM RoseEmployees;

SELECT COUNT(*)
FROM RoseStaticEmployees;

SELECT COUNT(*)
FROM RoseDynamicEmployees;

SELECT COUNT(*)
FROM RoseStaticEmployeesORC;

SHOW PARTITIONS RoseStaticEmployees;
SHOW PARTITIONS RoseDynamicEmployees;
SHOW PARTITIONS RoseStaticEmployeesORC;

CREATE TABLE IF NOT EXISTS RoseDynamicEmployees (
firstName string,
lastName string,
specialty string,
employeeNumber int
)
Partitioned by (dept string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC;
