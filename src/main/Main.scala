package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
// import CollaborativeItemModule.CollaborativeItemUser
// import MatrixFactorizationModule.{MatrixFactorizationRDD_ALS, MatrixFactorizationRDD}
import org.apache.spark.sql.{DataFrame}


object Main extends App {
  import spark.implicits._
  //❌💪
  
  val userId_selected = 849296
  val numMoviesRec = 5 
  var bucketName = "recommendation-system-lfag"
  val basePath = s"gs://$bucketName"
  val sentimentInputPath = s"$basePath/processed-dataset/user_reviews_quote_trunc.csv"
  val sentimentOutputPath = s"$basePath/processed-dataset/user_reviews_with_sentiment.csv"
  val matrixOutputPath = s"$basePath/processed-dataset/user_reviews_factorized_RDD_ALS.csv"
  val collabOutputPath = s"$basePath/processed-dataset/normalized_predicted_recommendations.csv"

  val spark = SparkSession.builder()
    .appName("Sentiment Analysis")
    .master("local[*]")
    .getOrCreate()
    
  // val sentimentInputPath = "processed/user_reviews_final_sampled.csv"
  // val sentimentOutputPath = "processed/user_reviews_with_sentiment.csv"

  //Augment Dataset with Sentiment Analysis
  // var sentimentDF = SentimentCSVProcessorSpark.processCSV(
  //   sentimentInputPath,
  //   sentimentOutputPath
  // ) 
  // sentimentDF.show(100, truncate = false)

  SentimentCSVProcessorSpark.processCSV(
    sentimentInputPath,
    sentimentOutputPath
  ) 

  //Matrix Factorization Recommendation
  // MatrixFactorizationRDD_ALS.matrixFactorizationRDDAls(
  //   userId_selected, 
  //   numMoviesRec,
  //   sentimentDF,
  //   matrixOutputPaths
  // )
  
  //Collaborative Filtering Recc. with MatrixFact. output
  // CollaborativeItemUser.main(Array(
  //   userId_selected.toString, 
  //   numMoviesRec.toString,
  //   sentimentOutputPath,
  //   collabOutputPath
  // ))
  
}