package com.clearurdoubt.parsers

import com.clearurdoubt.SparkInitializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{Row, SparkSession}

import java.time.LocalDate
import java.io.File

class TextFileParserSpec extends AnyFlatSpec with Matchers {
  val textFileParser = new TextFileParser
  implicit val spark: SparkSession = SparkInitializer("SparkETL_Test", "local[*]")

  "readData()" should "parse the given text file and return the DataFrame" in {
    val outputDF = textFileParser.readData("src/test/resources/sample_data/delimited_students_sample.txt")

    outputDF.collect() shouldBe Seq(Row("Sai Gowtham","Badvity",14,35), Row("Siddhartha","Badvity",40,46), Row("Sai Sanjay","Badvity",32,56))
  }

  "writeData()" should "write the DataFrame to the given location" in {
    val outputDF = textFileParser.readData("src/test/resources/sample_data/delimited_students_sample.txt")

    val curr_date = LocalDate.now
    textFileParser.writeData(outputDF, s"target/output/$curr_date")

    val dir = new File(s"target/output/$curr_date")
    dir.listFiles().exists(_.getName.endsWith(".csv")) shouldBe true
  }
}
