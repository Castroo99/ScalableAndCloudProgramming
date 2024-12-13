name := "collaborative-item"

description := "A demo application to showcase collaborative filtering using Apache Spark and Scala"

version := "0.1.0"

scalaVersion := "2.12.15" // Usa la versione compatibile con Spark

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
Compile / mainClass := Some("CollaborativeFilterPackage.CollaborativeFilteringDF")

// Dipendenze del progetto
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.apache.spark" %% "spark-mllib" % "3.4.0"
)

// Altre configurazioni opzionali
scalacOptions ++= Seq(
  "-deprecation", // Avvisi di deprecazione
  "-feature",     // Avvisi per funzionalità sperimentali
  "-unchecked",   // Controlli di compilazione meno stringenti
  "-Xlint"        // Avvisi estesi sugli errori
)
