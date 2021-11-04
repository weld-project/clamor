#!/bin/bash
flintrock launch --num-slaves $1 spark-flintrock
flintrock run-command spark-flintrock "aws s3 cp s3://weld-dsm-east/hadoop-aws-2.7.2.jar /home/ec2-user/spark/jars/"
flintrock run-command spark-flintrock "aws s3 cp s3://weld-dsm-east/aws-java-sdk-1.7.4.jar /home/ec2-user/spark/jars/"
#flintrock copy-file spark-flintrock ./spark-simple-sum/target/scala-2.11/spark-simple-sum_2.11-0.1.jar /home/ec2-user/
flintrock copy-file spark-flintrock ./spark-kmeans/target/scala-2.11/spark-kmeans_2.11-0.1.jar /home/ec2-user/
#flintrock copy-file spark-flintrock ./regex/target/scala-2.11/regex_2.11-0.1.jar /home/ec2-user/
#flintrock copy-file spark-flintrock ./spark-hashmap/target/scala-2.11/spark-hashmap_2.11-0.1.jar /home/ec2-user/
