class FpgaJsonParser {
  // --- Native methods
  @native def booleanMethod(b: Boolean): Boolean
  @native def setSchema(schemaFieldNames: String, schemaFieldTypes: Array[Int]): Boolean
  @native def parseJson(s: String): Array[Byte]
}

object FpgaJsonParser {

  // --- Main method to test our native library
  def main(args: Array[String]): Unit = {
    System.loadLibrary("FpgaJsonParser")
    val str = """{"NAME": "abcd","AGE": 10,"NUM": 7,"ADDRESS": "abcd"}\n{"NAME": "efgh","AGE": 8,"NUM": 8,"ADDRESS": "efgh"}"""
    val schemaFieldNames = "ID,TEXT"
    val schemaFieldTypes = Array(3,7)
    val parser = new FpgaJsonParser
    val bool = parser.booleanMethod(true)
    val bool2 = parser.setSchema(schemaFieldNames, schemaFieldTypes);
    val text = parser.parseJson(str)

    println(s"booleanMethod: $bool")
    println(text.deep.mkString("\n"))
  }

}
