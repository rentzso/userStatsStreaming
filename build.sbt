name := "userStatsStreaming"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" % "bijection-avro_2.11" % "0.9.5"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

mainClass in assembly := Some("insightproject.spark.userstatsstreaming")
