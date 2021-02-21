package com.clearurdoubt.streaming

import com.clearurdoubt.SparkInitializer

object StructuredStreaming extends App {
  implicit val spark = SparkInitializer("SparkStreaming", "local[2]")

  val df = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()

  df.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
}
