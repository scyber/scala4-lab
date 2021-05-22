import sbt._
import Keys._

/** dependencies */
object Dependency {
  val akkaHttpV = "10.1.3"
  val akkaStreamsV = "2.5.14"
  val scalaTestV = "3.0.4"
  val slickV = "3.2.3"
  val sparkV = "3.1.1"

  val spark = "org.apache.spark" %% "spark-core" % sparkV
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpV
  val akkaStreams = "com.typesafe.akka" %% "akka-stream" % akkaStreamsV
  val akkaHttpCore = "com.typesafe.akka" %% "akka-http-core" % akkaHttpV
  val akkaHttpSpray = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV


  val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.15.0"
  val scalalogginig = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3"
  //ToDo fix why can't use
  val logBack = "ch.qos.logback" % "logback-classic" % "1.2.3"



  // test dependencies
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestV % Test
}
