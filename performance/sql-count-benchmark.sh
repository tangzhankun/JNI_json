#!/bin/bash
unset SPARK_DIST_CLASSPATH
export SPARK_HOME=/opt/spark-2.2.1-SNAPSHOT-bin-json-spark-debug
export CL_CONTEXT_COMPILER_MODE_ALTERA=3
declare -a filePaths=("./100000.json"
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
