package simplesum

import org.apache.spark.{SparkConf, SparkContext}

object SimpleSum {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / 1e9 + " s")
    result
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleSum")
    val sc = new SparkContext(conf)

    println("Simple sum example")

    val rdd = sc.textFile("s3a://weld-dsm-2/ints-large.csv")
    val doubleRdd = rdd.flatMap(line=>line.split("\n"))
      .map(word=>word.toDouble)
      .cache()
    val tempSum = doubleRdd.sum

    val count = time {
      doubleRdd.sum
    }

    println(count)

    sc.stop()
  }
}
