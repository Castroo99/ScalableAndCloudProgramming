lazy val root = (project in file("."))
    .aggregate(main, sentimentAnalysis/*, matrixFactorization, collaborativeItem*/)
    .settings(
        name := "ScalableRecommendationSystem",
        mainClass in assembly := Some("main.Main"),
        assemblySettings
    )

lazy val main = (project in file("main"))
    .dependsOn(sentimentAnalysis/*, matrixFactorization, collaborativeItem*/)
    .settings(
        name := "Main",
        mainClass in Compile := Some("main.Main"), 
        mainClass in assembly := Some("main.Main"), 
        assemblySettings
    )

lazy val sentimentAnalysis = (project in file("sentimentAnalysis"))
    .settings(
        name := "SentimentCSVProcessorSpark",
        assemblySettings
    )

// lazy val matrixFactorization = (project in file("matrixFactorization"))
//     .settings(
//         name := "MatrixFactorizationRDD",
//         assemblySettings
//     )

// lazy val collaborativeItem = (project in file("collaborativeItem"))
//     .settings(
//         name := "CollaborativeItem",
//         assemblySettings
//     )

lazy val assemblySettings = Seq(
    assemblyShadeRules in assembly := Seq(
        ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x => MergeStrategy.first
    }
)