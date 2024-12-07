scalaVersion := "2.12.17" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-mllib" % "3.4.0"
)

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6" //% "provided"

// Google Cloud Storage client library
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "2.27.1"

// Kantan CSV (per gestire i file CSV)
libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.6.1"

// Spark Core (necessario per RDD, se non già incluso)
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"


mainClass in Compile := Some("MatrixFactorizationRDD")