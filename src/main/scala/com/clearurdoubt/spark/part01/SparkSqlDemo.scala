package com.clearurdoubt.spark.part01

import com.clearurdoubt.spark.SparkInitializer
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlDemo extends SparkInitializer {
  implicit val spark: SparkSession = getSparkSession(master = "local[*]", appName = "SparkDemo")

  def createSimpleDF(collection: Seq[Int]): DataFrame = {
    import spark.implicits._

    collection.toDF("id")
  }
}
