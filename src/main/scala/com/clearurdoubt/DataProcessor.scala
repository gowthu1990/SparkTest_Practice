package com.clearurdoubt

import com.clearurdoubt.parsers.TextFileParser

object DataProcessor {
  def main(args: Array[String]): Unit = {
    processIt(args)
  }

  def processIt(args: Array[String]): Unit = {
    require(args.length >= 2, "Insufficient number of parameters")

    implicit val spark = SparkInitializer("SparkETL", "local[*]")

    val txtParser = TextFileParser()

    val df = txtParser.readData(args(0))

    txtParser.writeData(df, args(1))
  }
}
