#!/bin/bash
export SPARK_HOME=/home/wasaisw1/forIntel/spark_integration/spark
for chars in 30
do
  #for lines in 10 100 1000 10000 100000 1000000 10000000
  for lines in 1500000
  do
  echo "Generating $lines JSON file with 37 colums($chars length field value) each line"
  $SPARK_HOME/bin/spark-submit \
    --class "SimpleApp" \
    --master local[1] \
    target/scala-2.11/fpga-json-performance_2.11-1.0.jar datagen $lines ./$lines-row-37col-$chars-char.json $chars
  sleep 5
  done
done
