import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"


scalaVersion := "2.12.18" // Spark 3.2.x supports Scala 2.12.x

// Define Spark version that has better compatibility with newer Java versions
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "com.github.jnr" % "jnr-posix" % "3.1.7",
  "joda-time" % "joda-time" % "2.10.10", // Use the latest version
   "org.apache.spark" %% "spark-streaming" % sparkVersion,
   "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
   "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,  // Kafka integration for Spark SQL
   "org.apache.hadoop" % "hadoop-aws" % "3.3.1"

)