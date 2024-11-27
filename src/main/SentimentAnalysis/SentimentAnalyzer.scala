package SentimentAnalysis

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import java.util.Properties
import scala.collection.convert.wrapAll._

object SentimentAnalyzer {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def mainSentiment(input: String): Double = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): Double = {
    val (_, sentimentValue) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentimentValue
  }

  def extractSentiments(text: String): List[(String, Double)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, getSentimentScore(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

  // Mappa il valore restituito da RNNCoreAnnotations in un valore numerico
  private def getSentimentScore(predictedClass: Int): Double = {
    predictedClass match {
      case 0 => 1  // molto negativo
      case 1 => 2  // negativo
      case 2 => 3   // neutro
      case 3 => 4   // positivo
      case 4 => 5   // molto positivo
      case _ => -1   // valore predefinito in caso di errore
    }
  }
}