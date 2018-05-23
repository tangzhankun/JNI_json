#!/bin/bash
unset SPARK_DIST_CLASSPATH
export SPARK_HOME=/root/code/spark
export BIGDL_HOME=/root/code/BigDL
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
  --jars /root/customer_support/JNI_practise/performance/bigdl-0.6.0-SNAPSHOT-jar-with-dependencies.jar \
  target/JSONFPGA-1.0-SNAPSHOT.jar sql-count $path false
sleep 3
done
