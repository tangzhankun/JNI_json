#!/bin/bash
export SPARK_HOME=/root/code/spark
$SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[1] \
  --driver-memory 6G \
  target/scala-2.11/fpga-json-performance_2.11-1.0.jar streaming /root/customer_support/WASAI/performance/streaming_input false
