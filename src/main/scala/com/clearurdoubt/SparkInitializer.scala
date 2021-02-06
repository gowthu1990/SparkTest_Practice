package com.clearurdoubt

import org.apache.spark.sql.SparkSession

object SparkInitializer {
  def apply(appName: String, master: String): SparkSession = {
    val spark = SparkSession
                  .builder
                  .appName(appName)
                  .master(master)
                  .getOrCreate()

    spark
  }
}
