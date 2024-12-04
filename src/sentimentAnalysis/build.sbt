name := "sentiment-analyzer"
description := "A demo application to showcase sentiment analysis using Stanford CoreNLP and Scala"
version  := "0.1.0"

scalaVersion := "2.12.15"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" % "provided" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.0" % "provided"// MLlib per il collaborative filtering

mainClass in Compile := Some("SentimentCSVProcessorSpark")