# spark-using-ojai-secondary-index-example

To build:
$ mvn clean install

To run:

Replace SPARK_VERSION with the version on the system.


$ /opt/mapr/spark/<SPARK_VERSION>/bin/spark-submit --class com.mapr.demo.spark.ojai.secondaryindex.SparkOjaiApplication --master yarn --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:///opt/mapr/conf/log4j.properties" --conf "spark.yarn.executor.memoryOverhead=1G"  --executor-memory 2G --num-executors 1 --executor-cores 1 spark-ojai-secondaryindex-1.0-SNAPSHOT.jar

