/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * Modified from KMeansExample from Spark MLLib examples
  */

package kmeans

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.random.RandomRDDs.{normalVectorRDD}

object kmeans {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toFloat / 1e9 + " s")
    result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // Load the data
    val bytesIn100GB: Long = 100000000000L;
    val d = 10;
    val k = 100;
    val rows = bytesIn100GB / (8 * d)
    println(rows)
    val data =
      normalVectorRDD(sc, rows, 10, 64).cache()
    val ct = data.count()
    println(ct)

    // Cluster the data into 100 classes using 10 iterations of KMeans
    val numIterations = 10
    val clusters = time {
      KMeans.train(data, k, numIterations, initializationMode = "random")
    }

    for ( c <- clusters.clusterCenters ) {
      println(c)
    }

    sc.stop()
  }
}
