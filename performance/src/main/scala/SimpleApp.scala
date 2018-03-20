import java.io.{BufferedOutputStream, ByteArrayOutputStream, FileOutputStream, PrintWriter}

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

  def micoBenchmark(spark : SparkSession, filepath : String, useFPGA : Boolean): Unit = {
    val jsonFile = filepath // Should be some file on your system
    val theSchema = StructType(
      StructField("ACC_NBR", StringType, true) ::
      StructField("OBILLING_TID", StringType, true) ::
      StructField("NBILLING_TID", StringType, true) ::
      StructField("OPER_TID", StringType, true) :: Nil
    )
    val smallDF = spark.read.schema(theSchema).format("json").load(jsonFile)
    smallDF.createOrReplaceTempView("gdi_mb")
    val start_time = System.currentTimeMillis()
    val sqlStr = "select count(OPER_TID), count(NBILLING_TID), count(OBILLING_TID), count(ACC_NBR) from gdi_mb"
    val ret = spark.sql(sqlStr)
    ret.show
    val end_time = System.currentTimeMillis()

    println("SQL sentence: " + sqlStr)
    println("CPU Micro-benchmark costs: " + (end_time - start_time) + " ms")

    if (useFPGA) {
      val smallDF = spark.read.schema(theSchema).format("json_FPGA").load(jsonFile)
      smallDF.createOrReplaceTempView("gdi_mb")
      val start_time = System.currentTimeMillis()
      val sqlStr = "select count(OPER_TID), count(NBILLING_TID), count(OBILLING_TID), count(ACC_NBR) from gdi_mb"
      val ret = spark.sql(sqlStr)
      ret.show
      val end_time = System.currentTimeMillis()

      println("FPGA Micro-benchmark costs: " + (end_time - start_time) + " ms")
    }

  }


  def rightPad(old: String): String = {
    val paddings = Array.fill[Byte](128-old.length)(0)
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

  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  def dataGen(spark: SparkSession, row_count: Int, path: String, valueSize: Int) : Unit = {
    val templateJsonFile = "./more-column.json" // Should be some file on your system
    val smallDF = spark.read.format("json").load(templateJsonFile)
    val schema = smallDF.schema
    val pw = new PrintWriter(path)

    val one_group_row = 10000
    val group_count = row_count/one_group_row
    val remaining_row = row_count % one_group_row
    if(group_count >= 1) {
      for (j <- 1 to group_count) {
        var someData = List[Row]()
        for (i <- 1 to one_group_row) {
          someData = someData :+ Row(randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
            randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize)
           )
        }
        val myDF = spark.createDataFrame(
          spark.sparkContext.parallelize(someData),
          schema
        )
        myDF.toJSON.collect().foreach((s: String) => {
          pw.write(s)
          pw.write("\n")
        })
      }
    }
    // remaining rows
    var someData = List[Row]()
    for (i <- 1 to remaining_row) {
      someData = someData :+ Row(randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
        randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
        randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize),
        randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize), randomAlphaNumericString(valueSize)
        )
    }
    val myDF = spark.createDataFrame(
      spark.sparkContext.parallelize(someData),
      schema
    )
    myDF.toJSON.collect().foreach((s: String) => {
      pw.write(s)
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

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Micro Benchmark of FPGA JSON IP").master("local[1]").getOrCreate()
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
      case "micro" => {
        val filePath = args(1)
        val useFPGA = args(2).toBoolean
        micoBenchmark(spark, filePath, useFPGA)
      }
    }

    spark.stop()
  }
}
