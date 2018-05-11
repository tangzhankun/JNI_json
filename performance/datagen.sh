#!/bin/bash
# to avoid CDH environment conflicts or will report connect HDFS error
unset SPARK_DIST_CLASSPATH
export SPARK_HOME=/opt/spark-2.2.1-SNAPSHOT-bin-json-spark-debug
for chars in 30
do
  #for lines in 10 100 1000 10000 100000 1000000 10000000
  for lines in 10
  do
  echo "Generating $lines JSON file with 10 colums(one line 512 bytes) each line"
  $SPARK_HOME/bin/spark-submit \
    --class "SimpleApp" \
    --master local[1] \
    target/scala-2.11/fpga-json-performance_2.11-1.0.jar datagen $lines /tmp/$lines-row-10col-total-512bytes.json $chars
  sleep 5
  done
done
