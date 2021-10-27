package com.clearurdoubt.spark.df.creation

import com.clearurdoubt.spark.SparkInitializer
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo extends SparkInitializer {
  implicit val spark: SparkSession = getSparkSession(master = "local[*]", appName = "SparkSqlDemo")
  import spark.implicits._

  def createSimpleDF(collection: Seq[Int]): DataFrame = {
    collection.toDF("id")
  }

  def createTuple3DF(collection: Seq[(Int, String, String)]): DataFrame = {
    collection.toDF("id", "first_name", "last_name")
  }
}
