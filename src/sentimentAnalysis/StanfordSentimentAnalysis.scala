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

object StanfordSentimentAnalysis {
    
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  
  //QUI IL 1000 è stato compilato come 20
  props.setProperty("parse.maxlen", "20")
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

  def saveDataFrameToCsv(df: DataFrame, outputPath: String): Unit = {
    val writer = CSVWriter.open(new File(outputPath))
    // Write the header
    writer.writeRow(df.columns)
    // Write the data
    var i = 0
    df.collect().foreach(row => {
      i = i + 1
      println("Row" + i)
      writer.writeRow(row.toSeq.map(_.toString))
      })
    writer.close()
  }

  // var index = 0
  val sentimentUDF = F.udf((quote: String) => {
    if (quote != null && quote.nonEmpty) {
      // index=index+1
      val shortQuote = if (quote.length > 1000) quote.substring(0, 1000) else quote
      // print(f"Analyzing quote: ${index}\n")
      BigDecimal(extractSentiment(shortQuote)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else {
      3.0
    }
  })

  // Funzione per processare il CSV e aggiungere il risultato del sentiment
  def processCSV(spark: SparkSession, inputFile: String, outputPath: String): Unit = {
    import spark.implicits._
    // Aggiungi il tempo di inizio
    val startTime = System.nanoTime()

    // Carica il CSV in un DataFrame di Spark
    val df = spark.read.option("header", "true").csv(inputFile)

    // Applica la UDF al DataFrame per creare la nuova colonna 'sentimentResult'
    val resultDF = df
      .repartition(200) // Partiziona per parallelizzare
      .withColumn("sentimentResult", sentimentUDF(F.col("quote")))
      .drop("quote")
    println("Sentiment analysis completed.")
    //resultDF.show(1000, truncate = false)
    
    // Aggiungi il tempo di fine
    val endTime = System.nanoTime()
    // Calcola e stampa il tempo di esecuzione
    val duration = (endTime - startTime) / 1e9d // In secondi
    println(s"Tempo di esecuzione: $duration secondi")
    resultDF
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)
  }
  

  def main(args: Array[String]): Unit = {
    //❌💪
    //val inputFile = "../../processed/user_reviews_final_sampled.csv" // Nome del file CSV di input
    var bucketName = "recommendation-system-lfag"
    val basePath = s"gs://$bucketName"
	  val datasetPath = s"${basePath}/processed-dataset/goodx10.csv"
	  val outputPath = s"${basePath}/processed-dataset/df1_sentiment.csv"

    // Chiamata alla funzione per processare il CSV
    //processCSV(datasetPath, outputPath)
  }
}
