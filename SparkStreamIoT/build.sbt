name := "SparkStreamIoT"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.0",
//  "org.apache.spark" % "spark-streaming-mqtt_2.10" % "1.6.0",
//  "org.apache.spark" % "spark-streaming-zeromq_2.10" % "1.6.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
)

