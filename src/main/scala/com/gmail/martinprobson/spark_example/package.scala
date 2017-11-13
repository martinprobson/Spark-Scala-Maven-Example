package com.gmail.martinprobson

import org.apache.spark.sql.SparkSession

package object spark_example {
  
  /**
   * Return spark session object
   */
  def getSession = SparkSession
      .builder
      .appName("SparkTest")
      .enableHiveSupport()
      .getOrCreate()
      
  /**
  * Return some information on the environment we are running in.
  */
  def versionInfo = {
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
        
    versionInfo
  }
  
  /*
	* Dump spark configuration for the current spark session.
	*/
  def getAllConf = {
      getSession.conf.getAll.map { case(k,v) => "Key: [%s] Value: [%s]" format (k,v)} mkString("","\n","\n")
  }
      
}