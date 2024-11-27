package SentimentAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object CollaborativeItem {

  def main(args: Array[String]): Unit = {

    // Configurazione Spark
    val conf = new SparkConf()
      .setMaster("local[*]")  // Imposta il master (modifica se necessario)
      .setAppName("CollaborativeFilteringExample")
      .set("spark.serializer", classOf[KryoSerializer].getName)  // Usa Kryo per la serializzazione
      .set("spark.kryo.registrationRequired", "true") // Richiede la registrazione delle classi
      .set("spark.kryo.classesToRegister", "java.nio.ByteBuffer")  // Registra ByteBuffer per Kryo

    // Crea la sessione Spark
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // Carica il dataset (ad esempio, ratings.csv) dove movieId e userId sono stringhe
    val data = spark.read.option("header", "true").csv("C:/Users/f.vece/Downloads/SentimentAnalysis/cleaned_output.csv")
      .select($"userId", $"movieId", $"rating")

    // Controllo dei dati
    data.show(5)

    // Indicizzazione delle colonne 'userId' e 'movieId'
    val userIndexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userIndex")
      .fit(data)

    val movieIndexer = new StringIndexer()
      .setInputCol("movieId")
      .setOutputCol("movieIndex")
      .fit(data)

    // Trasforma i dati con gli indici
    val indexedData = userIndexer.transform(data)
      .transform(movieIndexer.transform(_)) // Trasforma anche i film in indici

    // Modello di ALS per il filtraggio collaborativo
    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.1)
      .setUserCol("userIndex")
      .setItemCol("movieIndex")
      .setRatingCol("rating")
      .setColdStartStrategy("drop") // Gestisce i valori di previsione per i nuovi utenti/film

    // Allena il modello ALS
    val model = als.fit(indexedData)

    // Previsioni
    val predictions = model.transform(indexedData)

    // Valutazione del modello
    val evaluator = new RegressionEvaluator()
      .setLabelCol("rating")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Visualizza le prime 10 previsioni
    predictions.show(10)

    // Chiudi la sessione Spark
    spark.stop()
  }
}
