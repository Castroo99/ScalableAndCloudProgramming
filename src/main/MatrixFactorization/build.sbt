scalaVersion := "2.12.17" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.1"
)

mainClass in Compile := Some("MatrixFactorization")