#!/bin/sh
hostname_chunk=`hostname | cut -f1 -d"."`
flume-ng agent --conf-file ambari-log-task3-timaeudg.conf --name $hostname_chunk
