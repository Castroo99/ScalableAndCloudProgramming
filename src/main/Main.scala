package main

import org.apache.spark.sql.SparkSession
//import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFilteringDF
import MatrixFactorizationModule.{MatrixFactorizationRDD_ALS, MatrixFactorizationRDD}
import org.apache.spark.sql.{DataFrame}


object Main extends App {
  //❌💪
  
  val userId_selected = 447145
  val numMoviesRec = 5 
  var bucketName = "recommendation-system-lfag"
  val basePath = s"gs://$bucketName"
  val sentimentInputPath = s"$basePath/processed-dataset/user_reviews_quote_trunc.csv"
  val sentimentOutputPath = s"$basePath/processed-dataset/user_reviews_with_sentiment.csv"
  val matrixOutputPath = s"$basePath/processed-dataset/user_reviews_factorized_RDD_ALS.csv"
  val collabOutputPath = s"$basePath/processed-dataset/normalized_predicted_recommendations.csv"
  val finalRecOutputPath = s"$basePath/processed-dataset/final_recommendations.csv"

  val spark = SparkSession.builder()
    .appName("recomandation")
    .master("local[*]")
    .getOrCreate()
    
    /*   
    SentimentCSVProcessorSpark.processCSV(
      sentimentInputPath,
      sentimentOutputPath
    )  
  */

  //Matrix Factorization Recommendation
  MatrixFactorizationRDD_ALS.matrixFactorizationRDDAls(
    spark,
    userId_selected, 
    numMoviesRec,
    sentimentOutputPath,
    matrixOutputPath
  )
  
  //Collaborative Filtering Recc. with MatrixFact. output
  CollaborativeFilteringDF.execCollaborativeItem(
    spark,
    userId_selected, 
    numMoviesRec,
    sentimentOutputPath,
    collabOutputPath
  )
  val ratings1 = spark.read
      .option("header", true)
      .csv(matrixOutputPath)
  val ratings2 = spark.read
      .option("header", true)
      .csv(collabOutputPath)

  // Assicurati che le colonne siano nel formato corretto, convertendo "totalScore" in un tipo numerico
  val ratings1WithScore = ratings1.withColumn("totalScore", ratings1("totalScore"))
  val ratings2WithScore = ratings2.withColumn("totalScore", ratings2("totalScore"))

  // Unisci i due DataFrame su userId e movieId
  val joinedRatings = ratings1WithScore
      .join(ratings2WithScore, Seq("userId", "movieId"), "outer") // Unione di tipo outer per includere tutte le righe

  // Somma i totalScore da entrambe le tabelle (puoi usare `coalesce` per evitare valori nulli)
  val finalRatings = joinedRatings.withColumn(
      "totalScore",
      ratings1WithScore("totalScore") + ratings2WithScore("totalScore")
      )
  

  // Scrivi il risultato in un nuovo file CSV
  finalRatings
      .select("userId", "movieId", "totalScore")
      .write
      .option("header", "true")
      .csv(finalRecOutputPath)
  
}