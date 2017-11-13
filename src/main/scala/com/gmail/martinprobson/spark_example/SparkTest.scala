package com.gmail.martinprobson.spark_example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import com.gmail.martinprobson.hadoop.util.HDFSUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY
import com.gmail.martinprobson.spark_example

case class Employee(emp_no: Int,birth_date: String, first_name: String, last_name: String, gender: String, hire_date: String)
case class Title(emp_no: Int,title: String, from_date: String, to_date: String)


/**
* Example Spark application
*/
object SparkTest {

  private val spark = spark_example.getSession
  import spark.implicits._

  def main(args: Array[String]) {
    println(spark_example.versionInfo)
    println(spark_example.getAllConf)
    
    //TODO In order for this to work, the hive jars need to be on the classpath - can I just add them as a maven dependency???
    val employee = spark.table("employees")
//    val employee = spark.sql("SELECT * FROM employees")
    val title = spark.sql("SELECT * FROM titles")
            .select($"emp_no",$"title",$"from_date".cast("date"),$"to_date".cast("date"))
            .where("from_date <= current_date and to_date > current_date")
    employee.alias("employee")
            .select($"emp_no",$"first_name",$"last_name")
            .join(title.alias("title"),employee.col("emp_no").equalTo(title.col("emp_no")))
            .select("employee.emp_no","employee.first_name","employee.last_name","title.title")
            .collect
            .foreach(println)
    println("Done")
    

    import spark.implicits._

    val employee2 = spark.sql("SELECT * FROM employees").as[Employee]
    val title2 = spark.sql("SELECT * FROM titles")
            .as[Title]

    
  }





}
