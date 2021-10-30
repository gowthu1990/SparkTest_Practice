package com.clearurdoubt.spark.df.creation

import com.clearurdoubt.spark.SparkInitializer
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql002TextFileReader extends SparkInitializer with App {
  implicit val spark: SparkSession = getSparkSession(master = "local[*]", appName = "TextFileReader")

  val df: DataFrame = spark.read.format("text").load("src/main/resources/text.txt")
  df.show()
}
