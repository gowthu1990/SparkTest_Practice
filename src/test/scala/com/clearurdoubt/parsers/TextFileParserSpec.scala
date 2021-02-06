package com.clearurdoubt.parsers

import com.clearurdoubt.SparkInitializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row

class TextFileParserSpec extends AnyFlatSpec with Matchers {
  val textFileParser = new TextFileParser
  implicit val spark = SparkInitializer("SparkETL_Test", "local[*]")

  "readData()" should "parse the given text file and return the DataFrame" in {
    val outputDF = textFileParser.readData("src/test/resources/sample_data/delimited_students_sample.txt")

    outputDF.collect() shouldBe Seq(Row("Sai Gowtham","Badvity",14,35), Row("Siddhartha","Badvity",40,46), Row("Sai Sanjay","Badvity",32,56))
  }
}
