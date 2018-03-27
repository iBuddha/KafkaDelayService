name := "KafkaDelayService"

version := "0.1-SNAPSHOT"

organization := "kafka"

scalaVersion := "2.11.11"

resolvers += Resolver.mavenLocal

val ScalaTestVersion = "2.2.6"
val ScalamockVersion = "3.6.0"
val AkkaVersion = "2.4.18"
val KafkaVersion = "0.10.1.1"
val KafkaClientVersion = "0.10.1.2-seek"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.19"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-core" % "1.2.3"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % KafkaClientVersion
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % KafkaVersion exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12")
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % AkkaVersion
libraryDependencies += "org.apache.curator" % "curator-test" % "2.10.0"
libraryDependencies += "org.apache.curator" % "curator-framework" % "2.10.0"
libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.6.46"
libraryDependencies += "me.lemire.integercompression" % "JavaFastPFOR" % "0.1.11"
libraryDependencies += "org.scalatest" %% "scalatest" % ScalaTestVersion % "test->*"
libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % ScalamockVersion % "test->*"

test in assembly := {}

mainClass in assembly := Some("kafka.delay.message.Main")

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.discard //do not keep log4j.properties in assembly jar
  case "application.conf" => MergeStrategy.discard // do not keep application.conf
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
