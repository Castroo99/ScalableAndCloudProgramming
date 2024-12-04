package main

import SentimentAnalysisModule.SentimentCSVProcessorSpark
import CollaborativeItemModule.CollaborativeFiltering
import MatrixFactorizationModule.MatrixFactorizationRDD


object Main extends App {
  SentimentCSVProcessorSpark.main(Array[String]())
  MatrixFactorizationRDD.main(Array[String]())
  CollaborativeFiltering.main(Array[String]())
}