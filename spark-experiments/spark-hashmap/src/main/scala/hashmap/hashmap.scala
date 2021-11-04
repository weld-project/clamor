package hashmap

import java.util

import org.apache.spark.{SparkConf, SparkContext}

object hashmap {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / 1e9 + " s")
    result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HashMap")
    val sc = new SparkContext(conf)

    val nelements = 62500000
    // Load and parse the data
    val hm = new util.HashMap[Long, Long](nelements);
    val data = 1 to nelements
    val range = sc.parallelize(data, 512)
    println("Constructing map...")
    for (i <- 0 to nelements) {
      if (i % 10000 == 0) println(i)
      hm.put(i, i)
    }

    val bhm = sc.broadcast(hm)

    println("done constructing.")

    val res = time {
      range.map(s => bhm.value.get(s)).sum()
    }

    println(res)

    sc.stop()
  }
}
