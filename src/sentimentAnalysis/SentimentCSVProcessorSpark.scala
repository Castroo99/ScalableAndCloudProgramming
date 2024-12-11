package SentimentAnalysisModule

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import com.github.tototoshi.csv.CSVWriter
import java.nio.channels.Channels
import java.util.Properties
import java.io.{File, FileOutputStream, BufferedOutputStream, FileInputStream, BufferedInputStream}
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.math.BigDecimal.RoundingMode
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object SentimentCSVProcessorSpark {

  // Crea una sessione Spark
  val spark: SparkSession = SparkSession.builder()
    .appName("ReccSys")
    .master("local[4]")
    .getOrCreate()

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

  def saveDataFrameToGcs(dataFrame: DataFrame, outputPath: String): Unit = {
    try {
      // Save the DataFrame as a CSV file to the specified GCS path
      dataFrame
        .coalesce(1) // Combine partitions to create a single output file
        .write
        .option("header", "true") // Include header in the CSV
        .mode("overwrite") // Overwrite existing file if it exists
        .csv(outputPath) // Write to the specified GCS path
      
      println(s"DataFrame successfully saved to $outputPath")
    } catch {
      case e: Exception => 
        println(s"Error saving DataFrame to GCS: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Funzione per processare il CSV e aggiungere il risultato del sentiment
  def processCSV(inputFile: String, outputPath: String): DataFrame = {
    // Aggiungi il tempo di inizio
    val startTime = System.nanoTime()

    // Carica il CSV in un DataFrame di Spark
    val df = spark.read.option("header", "true").csv(inputFile)

    // Definisci una UDF (User Defined Function) per calcolare il sentiment
    val sentimentUDF = F.udf((quote: String) => {
      if (quote != null && quote.nonEmpty) {
        val sentiment = extractSentiment(quote)
        BigDecimal(sentiment).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      } else {
        3.0
      }
    })

    // Applica la UDF al DataFrame per creare la nuova colonna 'sentimentResult'
    val resultDF = df.withColumn("sentimentResult", sentimentUDF(F.col("quote"))).drop("quote")
    // Persisti il DataFrame in memoria
    // val cachedDF = resultDF.cache()

    // Stampa una parte del DataFrame per verificare i risultati
    //cachedDF.show(20, truncate = false)

    //saveDataFrameToGcs(finalDF, outputPath)
    
    // Aggiungi il tempo di fine
    val endTime = System.nanoTime()
    // Calcola e stampa il tempo di esecuzione
    val duration = (endTime - startTime) / 1e9d // In secondi
    println(s"Tempo di esecuzione: $duration secondi")

    resultDF
  }
  

  def main(args: Array[String]): Unit = {
    //❌💪
    //val inputFile = "../../processed/user_reviews_final_sampled.csv" // Nome del file CSV di input
	  val datasetPath = args(0)
	  val outputPath = args(1)

    // Chiamata alla funzione per processare il CSV
    processCSV(datasetPath, outputPath)
  }
}
