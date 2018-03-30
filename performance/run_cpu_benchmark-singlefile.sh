#!/bin/bash
# you need to use the tangzhankun' spark branch cpujson_benchmark
# https://github.com/tangzhankun/spark.git

export SPARK_HOME=/root/code/spark
$SPARK_HOME/bin/spark-submit \
  --class "org.apache.spark.examples.sql.SparkSQLExample" \
  --master local[1] \
  --driver-memory 6G \
  $SPARK_HOME/examples/target/scala-2.11/jars/spark-examples_2.11-2.2.1-SNAPSHOT.jar /root/customer_support/WASAI/performance/10000000-5row.json
