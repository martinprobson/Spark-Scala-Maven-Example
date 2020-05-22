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

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.metric.SQLMetric

object Run extends SparkEnvironment with Logging {

  def main(args: Array[String]) = {
    info("Start")
    info(s"Spark version: ${spark.version}")
    info(s"Max memory: ${Runtime.getRuntime.maxMemory}")
    info(s"Total memory: ${Runtime.getRuntime.totalMemory}")
    info(s"Free memory: ${Runtime.getRuntime.freeMemory}")
    import spark.implicits._
    val ds = spark.sparkContext.parallelize(1 to 100)
      .map{ (i: Int) => i % 2
        match { case 0 => (i,i)
        case _ => (2,i)}
      }.reduceByKey(_+_).toDS
    //val sparkEnv = org.apache.spark.SparkEnv.get
    println(ds.count)
    ds.show(10)
    //val emps = spark.sqlContext.emptyDataFrame
    //val emps: DataFrame = spark.sqlContext.read.json(getClass.getResource("/data/employees.json").getPath,
    //  getClass.getResource("/data/employees2.json").getPath).repartition(100)
    //println(emps.queryExecution.stringWithStats)
    //println(emps.queryExecution.executedPlan.toString())

    //val sp = emps.queryExecution.executedPlan.metrics.map { case(s,m) => println(s"s = $s ${m.toString()}")}
    //println(emps.count())
    //println("EXPLAIN")
    //println(emps.distinct().explain(true))
    //println("EXPLAIN END")

    //println("DEBUG")
    //val r = emps.rdd
    //println(r)
    //println(emps.explain(true))
    //println(emps.rdd.toDebugString)

    //println("DEBUG END")
    //info("End")

    System.in.read()
    spark.stop()
  }

}
