#!/bin/bash
#javah -v -cp /usr/share/scala/lib/scala-library.jar:. org.apache.spark.sql.execution.datasources.json.FpgaJsonParserImpl
rm libFpgaJsonParserImpl.so
g++ -fPIC -I$JAVA_HOME/include -I$JAVA_HOME/include/linux -o libFpgaJsonParserImpl.so -shared org_apache_spark_sql_execution_datasources_json_FpgaJsonParserImpl.cpp


