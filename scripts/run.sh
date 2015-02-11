#!/bin/sh
export 
# java -cp "jars/*" -Dspark.master=spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT} com.enremmeta.otter.spark.KMeansRunner $*

# spark-submit --class  com.enremmeta.otter.spark.KMeansRunner --master spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT} target/SimpleSpark-0.0.1-SNAPSHOT.jar $*
.  /etc/spark/conf/spark-env.sh
spark-submit --class  com.enremmeta.otter.spark.KMeansRunner --master local target/SimpleSpark-0.0.1-SNAPSHOT.jar $*

