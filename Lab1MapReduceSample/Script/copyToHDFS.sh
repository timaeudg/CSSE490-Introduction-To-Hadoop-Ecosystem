#!/bin/sh

if [ $# -ne 3 ]; then
    echo "usage: copyToHDFS <input directory> <output directory in HDFS> <output directory locally>"
    exit 0
fi

hadoop fs -put $1 $2;
hadoop fs -get $2 $3;
