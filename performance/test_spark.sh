#!/bin/bash
export SPARK_HOME=/opt/spark-2.2.1-SNAPSHOT-bin-json-spark-debug
  $SPARK_HOME/bin/spark-submit \
    --class "org.apache.spark.examples.SparkPi" \
    --master local[1] \
    /opt/spark-2.2.1-SNAPSHOT-bin-json-spark-debug/examples/jars/spark-examples_2.11-2.2.1-SNAPSHOT.jar 100

