#!/bin/sh
hostname_chunk=`hostname | cut -f1 -d"."`
flume-ng agent --conf-file timaeudgagentinterceptor.conf --name $hostname_chunk --classpath /tmp/timaeudg/TakeHomeExam2-0.0.1.jar -Xmx4096m -Xms2048m
