package com.clearurdoubt.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Test extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Spark Test")
    .getOrCreate()

  import spark.implicits._

 val df = spark.read    .format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/input.csv")

  df.show(false)

  val df3 = df.withColumn("rowNumber", dense_rank() over Window.partitionBy("ManagerId").orderBy(desc("AnnualSalary")))
  df3.show(false)

  df3.filter(col("rowNumber") === 2).select($"ID", $"Name", $"AnnualSalary", $"ManagerId").show(false)
}
