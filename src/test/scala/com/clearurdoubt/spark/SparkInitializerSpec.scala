package com.clearurdoubt.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkInitializerSpec extends AnyFlatSpec with Matchers {
  object SparkTest extends SparkInitializer {
    def apply(master: String, appName: String): SparkSession = getSparkSession(master, appName)
  }
  lazy val session: SparkSession = SparkTest("local[*]", "SparkTest")

  "getSparkSession" should "return a valid SparkSession" in {
    assert(session.isInstanceOf[SparkSession])
  }

  it should "return a SparkSession with given configuration" in {
    session.conf.get("spark.app.name") shouldBe "SparkTest"
    session.conf.get("spark.master") shouldBe "local[*]"
  }
}
