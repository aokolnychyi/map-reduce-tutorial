# Sample MapReduce Application

This repository contains a MapReduce application which is capable of identifying the greatest score of each individual player. 
It can be run in standalone and pseudo-distributed modes.

## Standalone mode
1. Build the project with a help of Gradle
`gradle build`
2. Run the application
`hadoop jar build/libs/map-reduce-examples-1.0-SNAPSHOT.jar com.aokolnychyi.mapreduce.MaxScoreDriver -conf src/main/resources/conf/hadoop-local.xml src/main/resources/input/sample.txt local-output`
3. Explore the output in the local-output directory

## Pseudo distributed mode
1. Build the project with a help of Gradle
`gradle build`
2. Start Hadoop
3. Create a folder in HDFS
`hdfs dfs -mkdir -p /user/{username}`
4. Copy the local input file to that folder in HDFS
`hadoop fs -copyFromLocal src/main/resources/input/sample.txt /user/{username}/sample.txt`
5. Run the application
`hadoop jar build/libs/map-reduce-examples-1.0-SNAPSHOT.jar com.aokolnychyi.mapreduce.MaxScoreDriver -conf src/main/resources/conf/hadoop-pseudo-distributed.xml /user/{username}/sample.txt cluster-output`
6. Retrieve the output
`hadoop fs -copyToLocal hdfs://localhost/user/{username}/cluster-output/ cluster-output`
7. Explore the output in the cluster-output directory