package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeItemUser
import MatrixFactorizationModule.{MatrixFactorizationRDD_ALS, MatrixFactorizationRDD}


object Main extends App {
  //❌💪
  val userId_selected = 849296
  val numMoviesRec = 5 
  var bucketName = "recommendation-system-lfag"
  val basePath = s"gs://$bucketName"
  val sentimentInputPath = s"$basePath/processed-dataset/user_reviews_final_sampled.csv"
  val sentimentOutputPath = s"$basePath/processed-dataset/user_reviews_with_sentiment.csv"
  val matrixOutputPath = s"$basePath/processed-dataset/user_reviews_factorized_RDD_ALS.csv"
  val collabOutputPath = s"$basePath/processed-dataset/normalized_predicted_recommendations.csv"

  //Augment Dataset with Sentiment Analysis
  SentimentCSVProcessorSpark.main(Array(
    sentimentInputPath,
    sentimentOutputPath
  )) 

  //Matrix Factorization Recommendation
  MatrixFactorizationRDD.main(Array(
    userId_selected.toString, 
    numMoviesRec.toString,
    sentimentOutputPath,
    matrixOutputPath
  ))
  
  //Collaborative Filtering Recc. with MatrixFact. output
  CollaborativeItemUser.main(Array(
    userId_selected.toString, 
    numMoviesRec.toString,
    sentimentOutputPath,
    collabOutputPath
  ))
  
}