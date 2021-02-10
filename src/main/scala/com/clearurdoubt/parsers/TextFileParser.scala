package com.clearurdoubt.parsers

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class TextFileParser {
  def readData(path: String)
              (implicit spark: SparkSession): DataFrame = {

    // Read the text file into an RDD
    val studentsRDD = spark.sparkContext.textFile(path)

    // Parse each line in the RDD according to the schema
    val studentsRowRDD = studentsRDD.map(line => Row(
      line.substring(0, 14).trim,
      line.substring(15, 29).trim,
      line.substring(30, 32).toInt,
      line.substring(33, 35).toInt))

    // Define the schema
    val schema = StructType(
      Array(
        StructField("firstName", StringType, nullable = true),
        StructField("lastName", StringType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("marks", IntegerType, nullable = true)
      )
    )

    // Create DF from RDD and Schema
    val studentsDF = spark.createDataFrame(studentsRowRDD, schema)

    studentsDF
  }

  def writeData(df: DataFrame, outputPath: String): Unit =

    // Write DF to a location
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(outputPath)
}

object TextFileParser {
  def apply(): TextFileParser =
    new TextFileParser
}
