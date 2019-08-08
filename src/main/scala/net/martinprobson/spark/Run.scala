package net.martinprobson.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

object Run extends App with SparkEnv with Logging {
  info("Start")
  val emps = spark.sqlContext.read.json(getClass.getResource("/data/employees.json").getPath)
  //println(emps.queryExecution.stringWithStats)
  println(emps.queryExecution.executedPlan.toString())
  //val sp = emps.queryExecution.executedPlan.metrics.map { case(s,m) => println(s"s = $s ${m.toString()}")}
  println(emps.count())
  info("End")

}
