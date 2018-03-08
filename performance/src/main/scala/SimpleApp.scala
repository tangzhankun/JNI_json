import org.apache.spark.sql.SparkSession

object SimpleApp {
  def micoBenchmark(spark : SparkSession): Unit = {
    val jsonFile = "./Small.json" // Should be some file on your system

    val smallDF = spark.read.format("json").load(jsonFile)
    smallDF.show()
    smallDF.createOrReplaceTempView("gdi_mb")
    val ret = spark.sql("select count(OPER_TID), count(NBILLING_TID), count(OBILLING_TID), count(ACC_NBR) from gdi_mb")
    ret.show()
  }
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Micro Benchmark of FPGA JSON IP").master("local[1]").getOrCreate()
    micoBenchmark(spark)
    spark.stop()
  }
}
