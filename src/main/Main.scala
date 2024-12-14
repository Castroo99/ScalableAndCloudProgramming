package main
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
//import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFilteringDF
import MatrixFactorizationALSPackage.MatrixFactorizationRDD_ALS
import org.apache.spark.sql.{DataFrame}
object Main extends App {
  //❌💪
  
  val userId_selected = 447145
  val numMoviesRec = 50 
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

  // Leggi i file CSV dei risultati
  val ratings1 = spark.read
    .option("header", true)
    .csv(matrixOutputPath)
  val ratings2 = spark.read
    .option("header", true)
    .csv(collabOutputPath)

  // Rinomina le colonne totalScore per evitare ambiguità
  val ratings1WithScore = ratings1.withColumnRenamed("totalScore", "totalScore1")
  val ratings2WithScore = ratings2.withColumnRenamed("totalScore", "totalScore2")
  
  // Unisci i due DataFrame su userId e movieId
  val joinedRatings = ratings1WithScore
    .join(ratings2WithScore, Seq("userId", "movieId"), "outer") // Unione di tipo outer per includere tutte le righe

  // Calcola il punteggio totale sommando i valori
  val finalRatings = joinedRatings.withColumn(
    "totalScore",
    coalesce(col("totalScore1"), lit(0)) + coalesce(col("totalScore2"), lit(0))
  )

  // Scrivi il risultato in un nuovo file CSV
  finalRatings
    .select("userId", "movieId", "totalScore")
    .write
    .option("header", "true")
    .mode("overwrite") // Overwrite existing file if it exists
    .csv(finalRecOutputPath) 
}