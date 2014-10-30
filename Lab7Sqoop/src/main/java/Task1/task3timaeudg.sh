#!/bin/sh

if [ $# -ne 1 ]
  then
    echo "Connection url for database expected"
    echo "Usage: task3timaeudg.sh <DB connection URL>"
fi

#Normal import
sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --split-by eid --target-dir /tmp/sqoopOutput
#Explicitly state number of mappers
sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees -m 1 --target-dir /tmp/sqoopMapOutput
#Writing as a sequence file
sqoop import --connect jdbc:mysql://$1/sqooptest --username root -m 2 --table Employees --split-by eid --target-dir /tmp/sqoopSeqOutput --as-sequencefile
#Warehouse dir
sqoop import --connect jdbc:mysql://$1/sqooptest --username root --table Employees --warehouse-dir /tmp/sqoop
#Change Delimiters & Null representation
sqoop import --connect jdbc:mysql://$1/sqooptest --username root --delete-target-dir -m 1 --table Employees --warehouse-dir /tmp/sqoop --fields-terminated-by '\t' --null-string 'This is a Null String' --null-non-string '\\N'
#create database in hive
hive -e "CREATE DATABASE IF NOT EXISTS sqooptest;"
#Import to Hive
sqoop import --connect jdbc:mysql://$1/sqooptest --username root -m 2 --table Employees --split-by eid --hive-import --create-hive-table --hive-table sqooptest.Employees
#Import to Hive with null replacement
sqoop import --connect jdbc:mysql://$1/sqooptest --username root -m 2 --table Employees --split-by eid --hive-import --create-hive-table --hive-table sqooptest.Employees --null-string 'This is a Null String'
#import all tables to warehouse
sqoop import-all-tables --connect jdbc:mysql://$1/sqooptest --username root -m 1 --warehouse-dir /tmp/sqoopAll/ --fields-terminated-by ',' --null-string '\\N' --null-non-string '\\N'