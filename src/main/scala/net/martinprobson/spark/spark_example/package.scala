package net.martinprobson.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import grizzled.slf4j.Logging

package object spark_example extends Logging {
  
  /**
   * Return spark session object
   * 
   * NOTE Add .master("local") to enable debug via eclipse or add as a VM option at runtime
   * -Dspark.master="local[*]"
   */
   def getSession = SparkSession.builder
      .appName("SparkTest")
      .getOrCreate()
      
  /**
  * Return some information on the environment we are running in.
  */
  def versionInfo: Seq[String] = {
    val sc = getSession.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")
    val sparkVersion = sc.version
    val sparkMaster  = sc.master
    val local        = sc.isLocal
    val defaultPar   = sc.defaultParallelism
    
    val versionInfo = s"""
        |---------------------------------------------------------------------------------
        | Spark version: $sparkVersion
        | Scala version: $scalaVersion
        | Spark master : $sparkMaster
        | Spark running locally? $local
        | Default parallelism: $defaultPar
        |---------------------------------------------------------------------------------
        |""".stripMargin
        
    versionInfo.split("\n")
  }
  
  /*
	* Dump spark configuration for the current spark session.
	*/
  def getAllConf: String = {
      getSession.conf.getAll.map { case(k,v) => "Key: [%s] Value: [%s]" format (k,v)} mkString("","\n","\n")
  }
      
}
