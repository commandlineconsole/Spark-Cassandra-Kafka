
name := "Spark-Cassandra-Kafka"

version := "0.3"

scalaVersion := "2.10.4"

//scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "1.6.1" withSources() withJavadoc(),
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" withSources() withJavadoc(),
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0" withSources() withJavadoc(),
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1"  withSources() withJavadoc(),
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" withSources() withJavadoc(),
  "com.github.nscala-time" %% "nscala-time" % "1.6.0" withSources() withJavadoc(),
  "org.apache.commons" % "commons-csv" % "1.1"
)

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.1"
libraryDependencies += "org.json" % "json" % "20160212"
libraryDependencies += ("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler")
// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.4.1"


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}