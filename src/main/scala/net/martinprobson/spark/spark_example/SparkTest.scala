package net.martinprobson.spark.spark_example

import java.io.InputStream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.gmail.martinprobson.hadoop.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
import grizzled.slf4j.Logging
import net.martinprobson.spark.spark_example


/**
* Example Spark application
*/
object SparkTest extends Logging {

  private val spark = spark_example.getSession
  import spark.implicits._  
  
  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
  
  private val empSchema = StructType(
                              StructField("emp_no",LongType,false) ::
                              StructField("birth_date",DateType,false) ::
                              StructField("first_name",StringType,true) ::
                              StructField("last_name",StringType,true) ::
                              StructField("gender",StringType,true) ::
                              StructField("hire_date",DateType,false) :: Nil)
  
  private val titlesSchema = StructType(
                              StructField("emp_no",LongType,false) ::
                              StructField("title",StringType,false) ::
                              StructField("from_date",DateType,true) ::
                              StructField("to_date",DateType,true) :: Nil)

  def main(args: Array[String]) {
    
      info("Starting...")
      spark_example.versionInfo.foreach(info(_))
      getSession.conf.getAll.foreach(info(_))

      
      val empsRDD = spark.sparkContext.parallelize(getInputData("/data/employees.json"),5)
      val empsDF  = spark.read.schema(empSchema).json(empsRDD)
      empsDF.createOrReplaceTempView("employees")
      
      val titlesRDD = spark.sparkContext.parallelize(getInputData("/data/titles.json"),5)
      val titlesDF  = spark.read.schema(titlesSchema).json(titlesRDD)
      titlesDF.createOrReplaceTempView("titles")
      
      val employee = spark.table("employees")
      val title = spark.sql("SELECT * FROM titles")
                       .select($"emp_no",$"title",$"from_date".cast("date"),$"to_date".cast("date"))
                       .where("from_date <= current_date and to_date > current_date")
      employee.alias("employee")
              .select($"emp_no",$"first_name",$"last_name")
              .join(title.alias("title"),employee.col("emp_no").equalTo(title.col("emp_no")))
              .select("employee.emp_no","employee.first_name","employee.last_name","title.title")
              .take(10)
              .foreach(println)
      info("Finished...")   
      spark.stop
  }
}
