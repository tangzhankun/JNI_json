we need to set these in spark's conf/spark-defaults.conf to use the FPGA library and
turn on the spark events in case we need analysis the spark job

mkdir /tmp/spark-events

In conf/spark-default
spark.driver.extraLibraryPath /root/OpenCL-FPGA/JNI_integration/JNI_json/bin
spark.eventLog.enabled           true
spark.eventLog.dir               file:///tmp/spark-events
