#!/bin/bash
# you need to use the tangzhankun' spark branch cpujson_benchmark
# https://github.com/tangzhankun/spark.git
export SPARK_HOME=/home/wasaisw1/forIntel/spark_integration/spark_micro

declare -a filePaths=("/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/10-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/100-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/1000-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/10000-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/100000-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/1000000-row-37col-30-char.json"
               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/1500000-row-37col-30-char.json"
               )
#declare -a filePaths=("/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/10-row-37col-30-char.json"
#               "/home/wasaisw1/forIntel/spark_integration/row_from_FPGA/JNI_json/performance/100000-row-37col-30-char.json"
#               )

for path in "${filePaths[@]}"
do
echo "ZK-----------" >> ./fpga-micro.log
echo "$path" >> ./fpga-micro.log
../a.out $path >> ./fpga-micro.log
done
