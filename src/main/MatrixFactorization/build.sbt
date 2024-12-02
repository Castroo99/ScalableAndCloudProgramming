scalaVersion := "2.12.17" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-mllib" % "3.4.1"
)

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in Compile := Some("MatrixFactorizationRDD")