package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFilteringUser
import MatrixFactorizationModule.MatrixFactorizationRDD_ALS


object Main extends App {
  
  //Augment Dataset with Sentiment Analysis
  SentimentCSVProcessorSpark.main(Array[String]()) 
  //Matrix Factorization Reccomendation
  MatrixFactorizationRDD_ALS.main(Array[String]())
  //Collaborative Filtering Recc. with MatrixFact. output
  CollaborativeFilteringUser.main(Array[String]())
  
}