package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFiltering
import MatrixFactorizationModule.MatrixFactorizationRDD_ALS


object Main extends App {
  //Augment Dataset with Sentiment Analysis
  SentimentCSVProcessorSpark.main(Array[String]()) 
  //Matrix Factorization Reccomendation
  MatrixFactorizationRDD_ALS.main(Array[String]())
  //Collaborative Filtering Recc. with MatrixFact. output
  //CollaborativeFiltering.main(Array[String]())
  
}