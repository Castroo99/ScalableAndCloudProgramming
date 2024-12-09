package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFilteringUser
import MatrixFactorizationModule.MatrixFactorizationRDD_ALS


object Main extends App {
  val userId_selected = 849296
  val numMoviesRec = 5 
  
  //Augment Dataset with Sentiment Analysis
  SentimentCSVProcessorSpark.main(Array[String]()) 

  //Matrix Factorization Recommendation
  MatrixFactorizationRDD_ALS.main(Array(
    userId_selected.toString, 
    numMoviesRec.toString,
    "recommendation-system-lfag",
    "processed-dataset/user_reviews_with_sentiment.csv",
    "processed-dataset/user_reviews_factorized_RDD_ALS.csv"
  ))
  
  //Collaborative Filtering Recc. with MatrixFact. output
  CollaborativeFilteringUser.main(Array[String]())
  
}