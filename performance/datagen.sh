#!/bin/bash
export SPARK_HOME=/root/code/spark
lines=10
$SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[1] \
  target/scala-2.11/fpga-json-performance_2.11-1.0.jar datagen $lines ./$lines-row-37col.json 5
