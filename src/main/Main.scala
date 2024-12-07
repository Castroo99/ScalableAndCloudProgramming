package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFiltering
import MatrixFactorizationModule.MatrixFactorizationRDD_ALS


object Main extends App {
  SentimentCSVProcessorSpark.main(Array[String]())
  MatrixFactorizationRDD_ALS.main(Array[String]())
  CollaborativeFiltering.main(Array[String]())
}