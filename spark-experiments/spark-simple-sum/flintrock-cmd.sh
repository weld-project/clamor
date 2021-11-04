echo ./bin/spark-submit --class simplesum.SimpleSum --executor-memory 30G --executor-cores 8 --master spark://$1:7077 --deploy-mode cluster /home/ec2-user/spark-simple-sum_2.11-0.1.jar 

