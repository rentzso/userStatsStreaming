# userStatsStreaming

part of the Insight Project [nexTop](https://github.com/rentzso/nextop)

Spark Streaming receives message posted by [the user Producer](https://github.com/rentzso/simulatedUser) and send them into Elasticsearch using the native client library [elasticsearch-hadoop](https://github.com/elastic/elasticsearch-hadoop)

## Build and run instructions:
```
sbt assembly
```

Example usage in standalone:
```
$SPARK_HOME/bin/spark-submit --class insightproject.spark.userstatsstreaming.UserStatsStreaming --master \
spark://`hostname`:7077 --jars userStatsStreaming-assembly-1.0.jar userStatsStreaming-assembly-1.0.jar \
my_topic my_group_id
```

