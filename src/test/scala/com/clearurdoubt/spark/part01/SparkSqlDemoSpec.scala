package com.clearurdoubt.spark.part01

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSqlDemoSpec extends AnyFlatSpec with Matchers {
  val list = List(1, 2, 3)
  val df: DataFrame = SparkSqlDemo.createDF(list)

  "createDF()" should "create a DataFrame with one column \"id\"" in {
    val columns = df.columns.toList

    columns.length shouldBe 1
    columns.head shouldBe "id"
  }

  it should "create a DataFrame with three rows" in {
    val actual = df.collect()

    actual shouldBe Array(Row(1), Row(2), Row(3))
  }
}
