name := "spark-kmeans"

version := "0.1"

scalaVersion := "2.11.0"
libraryDependencies ++= Seq ("org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-mllib" % "2.4.3",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.1")