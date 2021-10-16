package com.clearurdoubt.spark

import org.apache.spark.sql.SparkSession

trait SparkInitializer {
  def getSparkSession(master: String, appName: String): SparkSession =
    SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
}
