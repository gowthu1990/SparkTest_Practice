package com.clearurdoubt

import org.apache.spark.sql.SparkSession

object Test extends App {

  val spark = SparkSession
    .builder
    .appName("SparkTest")
    .master("local")
    .getOrCreate()

  val df = spark.read.format("text").load("src/main/resources/text.txt")
  df.show
}
