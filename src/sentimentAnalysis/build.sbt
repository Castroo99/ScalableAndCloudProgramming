name := "sentiment-analyzer"
description := "A demo application to showcase sentiment analysis using Stanford CoreNLP and Scala"
version  := "0.1.0"

scalaVersion := "2.12.15"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "2.27.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.0"// MLlib per il collaborative filtering
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.4.0"
libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.6.1"

mainClass in Compile := Some("SentimentCSVProcessorSpark")