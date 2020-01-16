package net.martinprobson.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkEnvironment {
  lazy private[spark] val conf  = ConfigFactory.load
  lazy private[spark] val spark = getSession

  /**
    * Return some information on the environment we are running in.
    */
  private[spark] def versionInfo: Seq[String] = {
    val sc = getSession.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")

    val versionInfo = s"""
                         |---------------------------------------------------------------------------------
                         | Scala version: $scalaVersion
                         | Spark version: ${sc.version}
                         | Spark master : ${sc.master}
                         | Spark running locally? ${sc.isLocal}
                         | Default parallelism: ${sc.defaultParallelism}
                         |---------------------------------------------------------------------------------
                         |""".stripMargin

    versionInfo.split("\n")
  }

  /**
    * Return spark session object
    *
    * NOTE Add .master("local") to enable debug via an IDE or add as a VM option at runtime
    * -Dspark.master="local[*]"
    */
  private def getSession: SparkSession = {
    val sparkSession = SparkSession.builder
      .appName(conf.getString("spark_example.app_name"))
      .master("local[*]")
      .getOrCreate()
    sparkSession
  }

  /*
	* Dump spark configuration for the current spark session.
	*/
  private[spark] def getAllConf: String = {
    getSession.conf.getAll.map { case(k,v) => "Key: [%s] Value: [%s]" format (k,v)} mkString("","\n","\n")
  }
}
