package net.martinprobson.spark

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.{Outcome, fixture}

class SparkTest extends fixture.FunSuite with Logging {

  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("Test-Spark-Local")
      .master("local[2]")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  test("empsRDD rowcount2") { spark =>
    val empsRDD = spark.sparkContext.textFile(getClass.getResource("/data/employees.json").getPath,2000)
    assert(empsRDD.count === 1000)
  }

  test("empsRDD rowcount") { spark =>
    val empsRDD = spark.sparkContext.parallelize(getInputData("/data/employees.json"), 5)
    assert(empsRDD.count === 1000)
  }

  test("titlesRDD rowcount") { spark =>
    val titlesRDD = spark.sparkContext.parallelize(getInputData("/data/titles.json"), 5)
    assert(titlesRDD.count === 1470)
  }

  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}
