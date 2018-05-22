package net.martinprobson.spark.spark_example

import java.io.InputStream

import grizzled.slf4j.Logging
import net.martinprobson.spark.spark_example
import org.apache.spark.sql.types._


/**
  * Example Spark application
  */
object SparkTest extends App with Logging {

  private val spark = spark_example.getSession

  import spark.implicits._

  info("Starting...")
  spark_example.versionInfo.foreach(info(_))
  spark.conf.getAll.foreach(info(_))

  val empsRDD = spark.sparkContext.parallelize(getInputData("/data/employees.json"), 5)
  val empsDF = spark.read.schema(empSchema).json(empsRDD)
  empsDF.createOrReplaceTempView("employees")

  val titlesRDD = spark.sparkContext.parallelize(getInputData("/data/titles.json"), 5)
  val titlesDF = spark.read.schema(titlesSchema).json(titlesRDD)
  titlesDF.createOrReplaceTempView("titles")

  val employee = spark.table("employees")
  val title = spark.sql("SELECT * FROM titles")
    .select($"emp_no", $"title", $"from_date".cast("date"), $"to_date".cast("date"))
    .where("from_date <= current_date and to_date > current_date")
  employee.alias("employee")
    .select($"emp_no", $"first_name", $"last_name")
    .join(title.alias("title"), employee.col("emp_no").equalTo(title.col("emp_no")))
    .select("employee.emp_no", "employee.first_name", "employee.last_name", "title.title")
    .take(10)
    .foreach(println)
  info("Finished...")
  spark.stop

  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }

  private def empSchema = StructType(
    StructField("emp_no", LongType, nullable = false) ::
      StructField("birth_date", DateType, nullable = false) ::
      StructField("first_name", StringType, nullable = true) ::
      StructField("last_name", StringType, nullable = true) ::
      StructField("gender", StringType, nullable = true) ::
      StructField("hire_date", DateType, nullable = false) :: Nil)

  private def titlesSchema = StructType(
    StructField("emp_no", LongType, nullable = false) ::
      StructField("title", StringType, nullable = false) ::
      StructField("from_date", DateType, nullable = true) ::
      StructField("to_date", DateType, nullable = true) :: Nil)


}
