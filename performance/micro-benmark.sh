#!/bin/bash
export SPARK_HOME=/home/wasaisw1/forIntel/spark_integration/spark
$SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[1] \
  --driver-memory 2G \
  --executor-memory 2G\
  target/scala-2.11/fpga-json-performance_2.11-1.0.jar micro ./1000000-5row.json true
