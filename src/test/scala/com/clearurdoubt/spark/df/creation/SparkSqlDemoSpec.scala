package com.clearurdoubt.spark.df.creation

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSqlDemoSpec extends AnyFlatSpec with Matchers {
  val numbers = List(1, 2, 3)
  val simpleDF: DataFrame = SparkSqlDemo.createSimpleDF(numbers)

  val tuples = List((101, "John", "Doe"), (102, "Andy", "Flower"), (103, "Grant", "Flower"))
  val tuple3DF: DataFrame = SparkSqlDemo.createTuple3DF(tuples)

  "createDF()" should "create a DataFrame with one column \"id\"" in {
    val columns = simpleDF.columns.toList

    columns.length shouldBe 1
    columns.head shouldBe "id"
  }

  it should "create a DataFrame with three rows" in {
    val actual = simpleDF.collect()
    simpleDF.show()

    actual shouldBe Array(Row(1), Row(2), Row(3))
  }

  "createTuple3DF()" should "create a DataFrame with three columns" in {
    val columns = tuple3DF.columns.toList

    columns.length shouldBe 3
    columns shouldBe Array("id", "first_name", "last_name")
  }

  it should "create a DataFrame with three rows" in {
    val actual = tuple3DF.collect()
    tuple3DF.show()

    actual shouldBe Array(Row(101, "John", "Doe"), Row(102, "Andy", "Flower"), Row(103, "Grant", "Flower"))
  }
}
