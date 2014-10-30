#!/bin/sh

if [ $# -ne 1 ]
  then
    echo "Connection url for database expected"
    echo "Usage: task4timaeudg.sh <DB connection URL>"
fi

//Export without updates
sqoop export --connect jdbc:mysql://$1/sqooptest --username root -m 1 --table EmployeesExportData --export-dir /tmp/sqoop/Employees --input-fields-terminated-by '\t' --input-null-string 'This is a Null String' --input-null-non-string '\\N'

//Export with updates
sqoop export --connect jdbc:mysql://$1/sqooptest --username root -m 1 --table EmployeesExportData --export-dir /tmp/sqoop/Employees --input-fields-terminated-by '\t' --input-null-string 'This is a Null String' --input-null-non-string '\\N' --update-key eid --update-mode allowinsert
