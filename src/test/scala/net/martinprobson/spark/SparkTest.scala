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

  test("Read the employees json file into an RDD and check the rowcount = 1000") { spark =>
    val empsRDD = spark.sparkContext.parallelize(getInputData("/data/employees.json"), 5)
    assert(empsRDD.count === 1000)
  }

  test("Read the titles json file into an RDD and check the rowcount = 1000") { spark =>
    val titlesRDD = spark.sparkContext.parallelize(getInputData("/data/titles.json"), 5)
    assert(titlesRDD.count === 1470)
  }

  test("Read the employees json file into a DataFrame and check the rowcount = 1000") { spark =>
    val empsDF = spark.read.json(this.getClass.getResource("/data/employees.json").getPath)
    assert(empsDF.count === 1000)
  }

  test("Read the titles json file into a DataFrame and check the rowcount = 1000") { spark =>
    val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getPath)
    assert(titlesDF.count === 1470)
  }

  test("Read the employees json file into a DataSet and check the rowcount = 1000") { spark =>
    import java.sql.Date

    import spark.implicits._
    val empsDS = spark
      .read
      .json(this.getClass.getResource("/data/employees.json").getPath)
      .map(r => Employee(r.getAs[Long]("emp_no"),
        r.getAs[String]("first_name"),
        r.getAs[String]("last_name"),
        Date.valueOf(r.getAs[String]("birth_date")),
        r.getAs[String]("gender"),
        Date.valueOf(r.getAs[String]("hire_date"))))
    assert(empsDS.count === 1000)
  }

  test("Read the titles json file into a DataSet and check the rowcount = 1470") { spark =>
    import java.sql.Date

    import spark.implicits._
    val titlesDS = spark
      .read
      .json(this.getClass.getResource("/data/titles.json").getPath)
      .map(r => Title(r.getAs[Long]("emp_no"),
        r.getAs[String]("title"),
        Date.valueOf(r.getAs[String]("from_date")),
        Date.valueOf(r.getAs[String]("to_date"))))
    assert(titlesDS.count === 1470)
  }
  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}
