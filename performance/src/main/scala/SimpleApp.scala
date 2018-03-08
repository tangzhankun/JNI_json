import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val jsonFile = "./Small.json" // Should be some file on your system
    val spark = SparkSession.builder.appName("Micro Benchmark of FPGA JSON IP").getOrCreate()
    val peopleDF = spark.read.format("json").load(jsonFile)
    peopleDF.show()
    spark.stop()
  }
}
