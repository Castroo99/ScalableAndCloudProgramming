name := "collaborative-item"
description := "A demo application to showcase sentiment analysis using Stanford CoreNLP and Scala"

version := "0.1"

scalaVersion := "2.12.17" // Usa la versione compatibile con Spark

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-mllib" % "3.4.0"
)