#!/bin/bash
# you need to use the tangzhankun' spark branch cpujson_benchmark
# https://github.com/tangzhankun/spark.git
# the last boolean is indicate if you want to warm up

export SPARK_HOME=/root/code/spark
nice -n -15 $SPARK_HOME/bin/spark-submit \
  --class "org.apache.spark.examples.sql.SparkSQLExample" \
  --master local[1] \
  --driver-memory 6G \
  $SPARK_HOME/examples/target/scala-2.11/jars/spark-examples_2.11-2.2.1-SNAPSHOT.jar /root/customer_support/WASAI/performance/1500000-row-37col-30-char.json true
