#!/bin/bash
#javah -v -cp /usr/share/scala/lib/scala-library.jar:. org.apache.spark.sql.execution.datasources.json.FpgaJsonParserImpl
rm libFpgaJsonParserImpl.so
export JAVA_HOME=/usr/java/jdk1.8.0_161
g++ -D OLD_FPGA_IP -fpermissive -fPIC -I$JAVA_HOME/include -I$JAVA_HOME/include/linux -lrt -o libFpgaJsonParserImpl.so -shared org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.cpp


