package lookups

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

import scala.util.Random

object lookups {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / 1e9 + " s")
    result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Lookups")
    val sc = new SparkContext(conf)

    println("Simple sum example")

    val dataSize = 1250000000 // 10 GB of int32
    val rdd = sc.parallelize(0 to dataSize, 800)
    val pairRdd = rdd.map(x => (x, x))
    val rp = new RangePartitioner(800, pairRdd)
    val partRdd = pairRdd.partitionBy(rp).cache()
    val ct = partRdd.count
    println(ct)

    val numIndexes = 1000;
    var sum = 0
    val count = time {
      for (i <- 0 to numIndexes) { // Lookups from driver into cluster.
        sum += partRdd.lookup(Random.nextInt(dataSize))(0)
      }
    }
    println(sum)
    sc.stop()
  }
}
