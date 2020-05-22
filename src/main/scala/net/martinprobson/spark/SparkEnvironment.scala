// Copyright (C) 2011-2020 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
      .config("spark.eventLog.enabled",value = true)
      .config("spark.eventLog.dir",getClass.getResource("/").getPath)
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
