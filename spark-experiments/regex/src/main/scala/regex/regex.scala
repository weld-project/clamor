package regex

import org.apache.spark.{SparkConf, SparkContext}

object regex {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / 1e9 + " s")
    result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Regex")
    val sc = new SparkContext(conf)

    val emailRegex = "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])".r
    val compiledRegex = emailRegex.pattern // Only compile once, but we still have cost of serializing compiled regex
    val broadcastedRegex = sc.broadcast(compiledRegex)

    // Load and parse the data
    val haystack = "testemail@testemail.com"
    val data = (1 to 625000000).toArray
    val range = sc.parallelize(data, 512)

    val matched = time {
      range.map(s => if (compiledRegex.matcher(haystack + s.toString()).matches()) 1 else 0 ).cache().sum()
    }

    println(matched)

    sc.stop()
  }
}
