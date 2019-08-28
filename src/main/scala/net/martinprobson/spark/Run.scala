package net.martinprobson.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.metric.SQLMetric

object Run extends App with SparkEnv with Logging {
  info("Start")
  val emps: DataFrame = spark.sqlContext.read.json(getClass.getResource("/data/employees.json").getPath)
  //println(emps.queryExecution.stringWithStats)
  println(emps.queryExecution.executedPlan.toString())
  //val sp = emps.queryExecution.executedPlan.metrics.map { case(s,m) => println(s"s = $s ${m.toString()}")}
  println(emps.count())
  println("EXPLAIN")
  println(emps.distinct().explain(true))
  println("EXPLAIN END")

  println("DEBUG")
  val r = emps.rdd
  println(r)
  println(emps.explain(true))
  println(emps.rdd.toDebugString)

  println("DEBUG END")
  info("End")

}
