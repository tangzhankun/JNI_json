import java.io._

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonParser}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object SimpleApp {
  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }
  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  def sqlCountBenchmark(spark : SparkSession, filepath : String, useFPGA : Boolean): Unit = {
    val file = new File("./sqlCountBenchmark-" + java.time.LocalDate.now.toString + ".log")
    val bw = new BufferedWriter(new FileWriter(file, true))
    bw.write("-----------\n")
    val jsonFile = filepath // Should be some file on your system
    val theSchema = StructType(
      StructField("BEHAVIOR_ID", StringType, true) ::
      StructField("ITEM_ID", StringType, true) ::
      StructField("USER_ID", StringType, true) :: Nil
    )
    val smallDF = spark.read.schema(theSchema).format("json").load(jsonFile)
    smallDF.createOrReplaceTempView("gdi_mb")
    val start_time = System.currentTimeMillis()
    val sqlStr = "select count(BEHAVIOR_ID), count(ITEM_ID), count(USER_ID) from gdi_mb"
    val ret = spark.sql(sqlStr)
    ret.show
    val end_time = System.currentTimeMillis()
    println("fileName: " + filepath)
    bw.write(filepath + "\n")
    println("SQL sentence: " + sqlStr)
    println("CPU SQL-Count-benchmark costs: " + (end_time - start_time) + " ms")
    bw.write("CPU(ms): " + (end_time - start_time) + "\n")
    if (useFPGA) {
      val smallDF = spark.read.schema(theSchema).format("json_FPGA").load(jsonFile)
      smallDF.createOrReplaceTempView("gdi_mb")
      val start_time = System.currentTimeMillis()
      val sqlStr = "select count(BEHAVIOR_ID), count(ITEM_ID), count(USER_ID) from gdi_mb"
      val ret = spark.sql(sqlStr)
      ret.show
      val end_time = System.currentTimeMillis()

      println("FPGA SQL-Count-benchmark costs: " + (end_time - start_time) + " ms")
      bw.write("FPGA(ms): " + (end_time - start_time) + "\n")
    }

    bw.write("\n\n")
    bw.close()
  }


  def rightPad(old: String, targetLength: Int=128): String = {
    val paddings = Array.fill[Byte](targetLength-old.length)(0)
    val newStr = old + (paddings.map(_.toChar)).mkString
    // this padd with space
    // val newStr = f"$old%-128s"
    println("fomr " + old + " to " + newStr + ", length:" + newStr.length)
    newStr
  }

  def generateRows(spark : SparkSession, binname : String): Seq[UnsafeRow] = {
    binname match {
      case "people.bin" => {
        val str = rightPad("hello,json")
        val rows = Seq(
          Row(1, 123, 123, str),
          Row(2, 123, 123, str),
          Row(3, 123, 123, str),
          Row(4, 123, 123, str),
          Row(5, 123, 123, str))
        rows.map(row => toUnsafeRow(row, Array(IntegerType, IntegerType, IntegerType, StringType)))
      }
      case "Small.bin" => {
        val rows = Seq(
          Row(rightPad("13886092665"), rightPad("B210701"), rightPad("B210701"), rightPad("1O0311G80610ydH10G00")),
          Row(rightPad("13886092665"), rightPad("B210701"), rightPad("B210701"), rightPad("1O0311G80610ydH10G00")),
          Row(rightPad("13886092665"), rightPad("B210701"), rightPad("B210701"), rightPad("1O0311G80610ydH10G00")),
          Row(rightPad("13886092665"), rightPad("B210701"), rightPad("B210701"), rightPad("1O0311G80610ydH10G00")),
          Row(rightPad("13886092665"), rightPad("B210701"), rightPad("B210701"), rightPad("1O0311G80610ydH10G00")))
        rows.map(row => toUnsafeRow(row, Array(StringType, StringType, StringType, StringType)))
      }
      case _ => Nil
    }
  }

  def generateUnsafeRowBinary(spark : SparkSession , binname: String) : Unit = {

    val unsafeRows = generateRows(spark, binname)
    val bos = new BufferedOutputStream(new FileOutputStream(binname))


    for (unsafeRow <- unsafeRows) {
      println("------decoded bytes---------")
      val bytes = unsafeRow.getBytes()
      var i = 0
      bytes.foreach(b => {
        print(b.toInt + ",")
        i += 1
        if (i % 8 == 0) {
          println("")
        }
      })
      println("")
      println("---------------")
      bos.write(bytes)
      bos.flush()
    }
    bos.close()

  }

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  def randomInt(max: Int): Long = {
    return util.Random.nextInt(max).toLong
  }

  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  def dataGen(spark: SparkSession, row_count: Int, path: String, valueSize: Int) : Unit = {
    val templateJsonFile = "./BigReal.json" // Should be some file on your system
    val smallDF = spark.read.format("json").load(templateJsonFile)
    val schema = smallDF.schema
    println("The schema is " + schema.toString)
    val pw = new PrintWriter(path)

    val one_group_row = 10000
    val group_count = row_count/one_group_row
    val remaining_row = row_count % one_group_row
    if(group_count >= 1) {
      for (j <- 1 to group_count) {
        var someData = List[Row]()
        for (i <- 1 to one_group_row) {
          // user_id, item_id, behavior_id should be total 512bytes(including schema names and colon, quotation mark)
          someData = someData :+ Row(randomInt(1000000).toString(), randomInt(10000).toString(),
            randomInt(6).toString(), randomInt(123).toString(),
            randomAlphaNumericString(75), randomAlphaNumericString(75), randomAlphaNumericString(75),
            randomAlphaNumericString(75), randomAlphaNumericString(75), randomAlphaNumericString(105)
            )

/*
          someData = someData :+ Row(randomInt(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomAlphaNumericString(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
            randomInt(valueSize), randomAlphaNumericString(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize)
           )
*/
        }
        val myDF = spark.createDataFrame(
          spark.sparkContext.parallelize(someData),
          schema
        )
        myDF.toJSON.collect().foreach((s: String) => {
          pw.write(rightPad(s,511))
          pw.write("\n")
        })
      }
    }
    // remaining rows
    var someData = List[Row]()
    for (i <- 1 to remaining_row) {
          someData = someData :+ Row(randomInt(1000000).toString(), randomInt(10000).toString(),
            randomInt(6).toString(), randomInt(123).toString(),
            randomAlphaNumericString(75), randomAlphaNumericString(75), randomAlphaNumericString(75),
            randomAlphaNumericString(75), randomAlphaNumericString(75), randomAlphaNumericString(105)
            )
/*
          someData = someData :+ Row(randomInt(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomAlphaNumericString(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
            randomInt(valueSize), randomAlphaNumericString(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize), randomInt(valueSize),
            randomInt(valueSize), randomInt(valueSize)
           )
*/
    }
    val myDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      schema
    )
    val tempStr = ""
    myDF.toJSON.collect().foreach((s: String) => {
      pw.write(rightPad(s,511))
      pw.write("\n")
    })

    pw.close()
    //val randomDF = spark.read.json(path)
    //randomDF.toJSON.collect().foreach(println)
  }

  def CPUJSONPerformance(sparkSession: SparkSession): Unit = {
//    val actulSchema = StructType()
//    val extraOptions = new scala.collection.mutable.HashMap[String, String]
//    val parsedOptions = new JSONOptions(
//      extraOptions.toMap,
//      sparkSession.sessionState.conf.sessionLocalTimeZone,
//      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
//    val rawParser = new JacksonParser(actualSchema, parsedOptions)
//    stringArray.foreach() {
//        rawParser.parse()
//      }
  }

  def sqlAgg(spark: SparkSession, filepath : String, useFPGA : Boolean): Unit = {
    val jsonFile = filepath // Should be some file on your system
    val theSchema = StructType(
      StructField("ACC_NBR", StringType, true) ::
        StructField("OBILLING_TID", StringType, true) ::
        StructField("NBILLING_TID", StringType, true) ::
        StructField("OPER_TID", StringType, true) :: Nil
    )
    val smallDF = spark.read.schema(theSchema).format("json").load(jsonFile)
    val start_time = System.currentTimeMillis()
    smallDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg", "OBILLING_TID" -> "min").show()
    val end_time = System.currentTimeMillis()

    println("CPU End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")

    if (useFPGA) {
      val aDF = spark.read.schema(theSchema).format("json_FPGA").load(jsonFile)
      val start_time = System.currentTimeMillis()
      aDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg").show()
      val end_time = System.currentTimeMillis()
      println("FPGA End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")
    }
  }

  def streamingEndToEndBenchmark(spark: SparkSession, filepath: String, useFPGA : Boolean): Unit = {
    val jsonFile = filepath // Should be some file on your system
    val theSchema = StructType(
      StructField("ACC_NBR", StringType, true) ::
        StructField("OBILLING_TID", StringType, true) ::
        StructField("NBILLING_TID", StringType, true) ::
        StructField("OPER_TID", StringType, true) :: Nil
    )
    val smallDF = spark.readStream.schema(theSchema).format("json").load(jsonFile)
    val start_time = System.currentTimeMillis()
    val query = smallDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg", "OBILLING_TID" -> "min").writeStream
      .outputMode("complete")
      .format("console")
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    val end_time = System.currentTimeMillis()
    println("[Streaming] CPU End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")

    if (useFPGA) {
      val smallDF = spark.readStream.schema(theSchema).format("json_FPGA").load(jsonFile)
      val start_time = System.currentTimeMillis()
      // we must declare four functions on four columns because our FPGA returns fixed 4 columns data
      val query = smallDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg", "OBILLING_TID" -> "min").writeStream
        .outputMode("complete")
        .format("console")
        .start()
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      val end_time = System.currentTimeMillis()
      println("[Streaming] FPGA End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")
    }
  }

  def streamingFromKafkaEndToEndBenchmark(spark: SparkSession, filepath: String, useFPGA : Boolean): Unit = {
    val jsonFile = filepath // Should be some file on your system
    val theSchema = StructType(
      StructField("ACC_NBR", StringType, true) ::
        StructField("OBILLING_TID", StringType, true) ::
        StructField("NBILLING_TID", StringType, true) ::
        StructField("OPER_TID", StringType, true) :: Nil
    )
    val smallDF = spark.readStream.schema(theSchema).format("json").load(jsonFile)
    val start_time = System.currentTimeMillis()
    val query = smallDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg", "OBILLING_TID" -> "min").writeStream
      .outputMode("complete")
      .format("console")
      .start()
    try {
      smallDF.write.parquet("/tmp/user_actions.parquet")
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    val end_time = System.currentTimeMillis()
    println("[Streaming] CPU End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")

    if (useFPGA) {
      val smallDF = spark.readStream.schema(theSchema).format("json_FPGA").load(jsonFile)
      val start_time = System.currentTimeMillis()
      // we must declare four functions on four columns because our FPGA returns fixed 4 columns data
      val query = smallDF.agg("NBILLING_TID" -> "min", "ACC_NBR" -> "max", "OPER_TID" -> "avg", "OBILLING_TID" -> "min").writeStream
        .outputMode("complete")
        .format("console")
        .start()
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      val end_time = System.currentTimeMillis()
      println("[Streaming] FPGA End-To-End-Benchmark costs: " + (end_time - start_time) + " ms")
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark FPGA Json Test").master("local[1]").getOrCreate()
    args(0) match {
      case "datagen" => {
        val row_count = args(1).toInt
        val pathName = args(2)
        val valueSize = args(3).toInt
        dataGen(spark, row_count, pathName, valueSize)
      }
      case "bingen" => {
        generateUnsafeRowBinary(spark, "people.bin")
        generateUnsafeRowBinary(spark, "Small.bin")
      }
      case "sql-count" => {
        val filePath = args(1)
        val useFPGA = args(2).toBoolean
        sqlCountBenchmark(spark, filePath, useFPGA)
      }
      case "sql-agg" => {
        val filePath = args(1)
        val useFPGA = args(2).toBoolean
        sqlAgg(spark, filePath, useFPGA)
      }
      case "streaming" => {
        val filePath = args(1)
        val useFPGA = args(2).toBoolean
        streamingEndToEndBenchmark(spark, filePath, useFPGA)
      }
      case "streaming_kafka" => {
        val filePath = args(1)
        val useFPGA = args(2).toBoolean
        streamingFromKafkaEndToEndBenchmark(spark, filePath, useFPGA)
      }
    }

    spark.stop()
  }
}
