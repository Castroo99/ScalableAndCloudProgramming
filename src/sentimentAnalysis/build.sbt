name := "sentiment-analyzer"

description := "A demo application to showcase sentiment analysis using Stanford CoreNLP and Scala"

version := "0.1.0"

scalaVersion := "2.12.15" // Assicurati che questa sia compatibile con le dipendenze

// Configurazione del plugin Assembly (se stai usando sbt-assembly)
enablePlugins(AssemblyPlugin)

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Configurazione del Main Class per l'assembly jar
Compile / mainClass := Some("SentimentAnalysisModule.old_SentimentCSVProcessorSpark")

// Dipendenze del progetto
libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" artifacts (
    Artifact("stanford-corenlp", "models"),
    Artifact("stanford-corenlp")
  ),
  "com.github.tototoshi" %% "scala-csv" % "1.3.6",
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "com.google.cloud" % "google-cloud-storage" % "2.27.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.0",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.4.0",
  "com.nrinaudo" %% "kantan.csv" % "0.6.1"
)

// Altre configurazioni opzionali
scalacOptions ++= Seq(
  "-deprecation", // Avvisi di deprecazione
  "-feature",     // Avvisi per funzionalità sperimentali
  "-unchecked",   // Controlli di compilazione meno stringenti
  "-Xlint"        // Avvisi estesi sugli errori
)
