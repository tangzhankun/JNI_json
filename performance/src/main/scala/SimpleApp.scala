import java.io.{BufferedOutputStream, ByteArrayOutputStream, FileOutputStream}

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

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

  def micoBenchmark(spark : SparkSession): Unit = {
    val jsonFile = "./Small.json" // Should be some file on your system

    val smallDF = spark.read.format("json").load(jsonFile)
    smallDF.show()
    smallDF.createOrReplaceTempView("gdi_mb")
    val ret = spark.sql("select count(OPER_TID), count(NBILLING_TID), count(OBILLING_TID), count(ACC_NBR) from gdi_mb")
    ret.show()
  }


  def generateUnsafeRowBinary() : Unit = {
    val str = "hello,json"
    val strLen = str.length
    val paddings = Array.fill[Byte](128-(strLen + 7)/8 * 8)(0)
    println("padding size is " + paddings.size)
    val rows = Seq(
      Row(1, 123, 123, str),
      Row(2, 123, 123, str),
      Row(3, 123, 123, str),
      Row(4, 123, 123, str),
      Row(5, 123, 123, str))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(IntegerType, IntegerType, IntegerType, StringType)))
    val bos = new BufferedOutputStream(new FileOutputStream("./unsafeRow.bin"))


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

      var j = 0
      paddings.foreach(b => {
        print(b.toInt + ",")
        j += 1
        if (j % 8 == 0) {
          println("")
        }
      })
      println("printed " + j + " padding 0 value, " + "total " + (i+j) + " bytes each line")
      println("")
      println("---------------")
      bos.write(bytes)
      bos.write(paddings)
      bos.flush()
    }
    bos.close()

  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Micro Benchmark of FPGA JSON IP").master("local[1]").getOrCreate()
    //micoBenchmark(spark)
    generateUnsafeRowBinary
    spark.stop()
  }
}
