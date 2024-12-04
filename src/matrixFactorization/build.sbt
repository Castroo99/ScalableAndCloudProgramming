scalaVersion := "2.12.17" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-mllib" % "3.4.0"
)

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6" % "provided"

mainClass in Compile := Some("MatrixFactorizationRDD")