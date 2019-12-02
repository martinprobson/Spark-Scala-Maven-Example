package net.martinprobson.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object Run extends App with SparkEnv with Logging {

  import spark.implicits._

  def genDataFrame: DataFrame = {
    import spark.implicits._
    val rnd = new scala.util.Random
    val l = List.fill(10)(rnd.nextInt(10000))
    println(l.size)
    spark.sparkContext.parallelize(l).toDF()
  }

  info("Start")
  val e: Seq[Int] = Seq()
  val empty = spark.sqlContext.createDataset(e).toDF()
  val l =Range(1,200).toList.map{ _ => genDataFrame}.reduce(_.union(_))
  println(l.count)

  info("End")
  scala.io.StdIn.readChar()
  spark.stop()

}
