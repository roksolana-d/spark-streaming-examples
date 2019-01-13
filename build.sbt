name := "SparkStreamingExamples"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.3.1"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "2.1.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "com.typesafe" % "config" % "1.3.3",
  "org.twitter4j" % "twitter4j-core" % "4.0.7",
  "org.twitter4j" % "twitter4j-stream" % "4.0.7",
  "com.twitter" %% "bijection-avro" % "0.9.6",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.2",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.0",
  "org.json4s" %% "json4s-native" % "3.6.2",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.5.3",
  "com.github.nscala-time" %% "nscala-time" % "2.20.0",
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1"
)