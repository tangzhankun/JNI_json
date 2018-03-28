#!/bin/bash
export SPARK_HOME=/home/wasaisw1/forIntel/spark_integration/spark
declare -a filePaths=("./10-row-37col-30-char.json"
		"./100-row-37col-30-char.json"
		"./1000-row-37col-30-char.json"
		"./10000-row-37col-30-char.json"
		"./100000-row-37col-30-char.json"
		"./1000000-row-37col-30-char.json"
		"./1500000-row-37col-30-char.json"
		)
for path in "${filePaths[@]}"
do
echo "sql count benchmark on $path"
$SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[1] \
  --driver-memory 6G \
  target/scala-2.11/fpga-json-performance_2.11-1.0.jar sql-count $path true
sleep 3
done
