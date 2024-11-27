package SentimentAnalysis

import com.github.tototoshi.csv._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import java.util.Properties
import scala.collection.JavaConversions.asScalaBuffer

object SentimentCSVProcessor {

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  // Funzione per ottenere il punteggio del sentiment in una scala da 1 a 5
  def getSentimentScore(predictedClass: Int): Double = {
    predictedClass match {
      case 0 => 1.0  // Molto negativo
      case 1 => 2.0  // Negativo
      case 2 => 3.0  // Neutro
      case 3 => 4.0  // Positivo
      case 4 => 5.0  // Molto positivo
      case _ => 3.0  // Default neutro
    }
  }

  // Funzione per ottenere il sentiment da una frase
  def extractSentiment(text: String): Double = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    val sentimentScores = sentences.map { sentence =>
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      getSentimentScore(RNNCoreAnnotations.getPredictedClass(tree))
    }
    // Restituisce il punteggio medio del sentiment per tutte le frasi
    if (sentimentScores.isEmpty) 3.0 else sentimentScores.sum / sentimentScores.size
  }

  def processCSV(inputFile: String, outputFile: String): Unit = {
    // Legge il CSV
    val reader = CSVReader.open(new java.io.File(inputFile))
    val rows = reader.allWithHeaders()

    // Aggiunge il risultato dell'analisi del sentiment alla colonna 'quote'
    val updatedRows = rows.map { row =>
      val quote = row.getOrElse("quote", "")
      val sentimentScore = if (quote.nonEmpty) extractSentiment(quote) else 3.0 // Se il testo è vuoto, assegna 3.0 (neutro)
      println(sentimentScore)
      // Aggiungiamo il risultato come nuova colonna 'sentimentResult'
      row + ("sentimentResult" -> sentimentScore.toString)
    }

    // Scrive il nuovo CSV con la colonna aggiuntiva
    val writer = CSVWriter.open(new java.io.File(outputFile))
    writer.writeAll(updatedRows.map(_.values.toList)) // scrive tutte le righe nel CSV
    writer.close()

    println(s"File elaborato e salvato in: $outputFile")
  }

  def main(args: Array[String]): Unit = {
    val inputFile = "C:/Users/f.vece/Downloads/output_1000.csv" // Nome del file CSV di input
    val outputFile = "output_with_sentiment.csv" // Nome del file CSV di output
    processCSV(inputFile, outputFile)
  }
}
