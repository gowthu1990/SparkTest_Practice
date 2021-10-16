package com.clearurdoubt.spark.df.creation

import com.clearurdoubt.spark.SparkInitializer
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo extends SparkInitializer {
  implicit val spark: SparkSession = getSparkSession(master = "local[*]", appName = "SparkDemo")

  def createSimpleDF(collection: Seq[Int]): DataFrame = {
    import spark.implicits._

    collection.toDF("id")
  }

  def createTuple3DF(collection: Seq[(Int, String, String)]): DataFrame = {
    import spark.implicits._

    collection.toDF("id", "first_name", "last_name")
  }
}
