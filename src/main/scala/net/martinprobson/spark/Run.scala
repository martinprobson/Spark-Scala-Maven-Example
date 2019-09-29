package net.martinprobson.spark

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.metric.SQLMetric

object Run extends App with SparkEnv with Logging {
  info("Start")
  //val emps = spark.sqlContext.read.json(getClass.getResource("/data/employees.json").getPath)
  spark.conf.set("spark.sql.codegen.wholeStage", false)
  val emps = spark.sqlContext.range(1,100,1,1)
  //println(emps.queryExecution.stringWithStats)
  println(emps.queryExecution.executedPlan.toString())
  //val sp = emps.queryExecution.executedPlan.metrics.map { case(s,m) => println(s"s = $s ${m.toString()}")}
//  println(emps.count())
  emps.take(10)
  println(emps.rdd.toDebugString)
  val r: RDD[_] = emps.rdd.dependencies.head.rdd
  println(r)
  info("End")

  System.in.read()
  spark.stop()

}
