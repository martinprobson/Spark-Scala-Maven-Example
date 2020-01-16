package net.martinprobson.spark

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.spark.SparkEnv
import org.apache.spark.memory.MemoryMode
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

  test("SparkEnv is avalable after initializing a spark context") { _ =>
    println(SparkEnv.get.memoryManager.maxOnHeapStorageMemory / 1024 / 1024)
    assert(SparkEnv.get.memoryManager.tungstenMemoryMode === MemoryMode.ON_HEAP)
  }

  test("DataFrame reader") { spark =>
    val name = getClass.getResource("/data/employees.json").getPath
    val emps = spark.sqlContext.read.json(name)
//    println(emps.count)
//    System.in.read()
    assert(emps.count === 1000)
  }

  test("empsDF rowcount") { spark =>
    import spark.implicits._
    val empsDS = spark.sqlContext.createDataset(getInputData("/data/employees.json"))
    assert(empsDS.count === 1000)
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
