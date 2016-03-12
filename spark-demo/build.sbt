name := """spark-demo"""

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-target:jvm-1.7"

javacOptions += "-target:jvm-1.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"
