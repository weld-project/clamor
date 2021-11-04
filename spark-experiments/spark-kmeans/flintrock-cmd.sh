echo ./bin/spark-submit --class kmeans.kmeans --executor-memory 30G --executor-cores 8 --master spark://$1:7077 --deploy-mode cluster /home/ec2-user/spark-kmeans_2.11-0.1.jar 

