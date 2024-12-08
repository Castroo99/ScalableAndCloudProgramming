package CollaborativeItemModule

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object CollaborativeItemUser {
  def main(args: Array[String], csvInputPath: String, csvOutputPath: String, targetUserId: String, n: Int ): Unit = {
    var bucketName = "recommendation-system-lfag"
	  var moviesFile = "full-dataset/movies.csv"
    var outputFile = "processed-dataset/normalized_predicted_recommendations.csv"

    val basePath = s"gs://$bucketName"
	  val datasetPath = s"$basePath/$inputFile"
	  val outputPath = s"$basePath/$outputFile"

    // Crea la SparkSession
    val spark = SparkSession.builder()
      .appName("CollaborativeFiltering")
      .master("local[4]") // Usa tutti i core disponibili
      .getOrCreate()

    // Importa le implicite necessarie per lavorare con DataFrame
    import spark.implicits._

    // Carica i dati dei film e delle recensioni
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvInputPath) // Adatta il percorso del tuo file
      .select("movieId", "userId", "rating", "sentimentResult")

    // Calcola il rating combinando il rating e il sentiment
    val sentimentDF = moviesDF.withColumn(
      "rating",
      col("rating") * 0.5 + col("sentimentResult") * 0.5
    )

    // ** Crea una matrice utente-film (user-item matrix) ** utilizzando tutto il dataset
    val userMovieRatings = sentimentDF
      .groupBy("userId", "movieId")
      .agg(avg("rating").as("rating"))
      .na.fill(0) // Riempie i valori nulli con 0

    val userMovieMatrix = userMovieRatings
      .groupBy("userId")
      .pivot("movieId")
      .agg(first("rating"))
      .na.fill(0) // Riempie i valori nulli con 0

    // ** Crea la mappatura degli indici ai movieId **
    val movieIds = userMovieMatrix.columns.drop(1) // Escludiamo la colonna userId
    val movieIdMapping = movieIds.zipWithIndex.map { case (movieId, idx) => idx.toLong -> movieId }.toMap

    // Broadcast della mappatura per l'uso nei calcoli
    val movieIdMappingBroadcast = spark.sparkContext.broadcast(movieIdMapping)

    // ** Conversione in RDD per il calcolo delle similarità tra film **
    val ratingRDD: RDD[Vector] = userMovieMatrix
      .select(movieIds.map(col): _*)
      .rdd
      .map(row => Vectors.dense(row.toSeq.toArray.map(_.toString.toDouble)))

    // ** Creazione della matrice indicizzata per il calcolo della similarità **
    val indexedMatrix = new IndexedRowMatrix(
      ratingRDD.zipWithIndex.map { case (vec, idx) => IndexedRow(idx, vec) }
    )

    // ** Calcola la similarità tra gli item (film) **
    val similarityMatrix = indexedMatrix.toCoordinateMatrix.transpose().toRowMatrix.columnSimilarities(0.1)

    // ** Calcola le predizioni basate sulla similarità tra i film **
    val recommendationsRDD = similarityMatrix.entries
      .map { entry =>
        // Usa la mappatura per convertire gli indici nei movieId originali
        val movieId1 = movieIdMappingBroadcast.value(entry.i)
        val movieId2 = movieIdMappingBroadcast.value(entry.j)
        (movieId1, movieId2, entry.value)
      }

    // ** Converte RDD[(String, String, Double)] in un DataFrame **
    val recommendationsRowsRDD = recommendationsRDD.map { case (movieId1, movieId2, similarity) =>
      Row(movieId1, movieId2, similarity)
    }

    val recommendationsSchema = Seq(
      "movieId1" -> StringType,
      "movieId2" -> StringType,
      "similarity" -> DoubleType
    )

    val recommendationsWithSimilarityDF = spark.createDataFrame(recommendationsRowsRDD,
      StructType(
        recommendationsSchema.map { case (name, dataType) =>
          StructField(name, dataType, nullable = false)
        }
      )
    )

    // Debug: Controlla quante raccomandazioni ci sono
    println(s"Numero di raccomandazioni generate: ${recommendationsWithSimilarityDF.count()}")

    // ** Unisci le raccomandazioni con l'utente target **
    // Filtro per l'utente target
    val userRatings = sentimentDF.filter($"userId" === targetUserId)

    // Filtra per i film che l'utente non ha visto
    val unseenMoviesDF = recommendationsWithSimilarityDF
      .join(userRatings, recommendationsWithSimilarityDF("movieId2") === userRatings("movieId"), "left_anti")

    // Calcola la previsione per ogni film non visto
    val recommendationsForUser = unseenMoviesDF
      .groupBy("movieId1")
      .agg(sum("similarity").as("predictedScore"))
      .orderBy(desc("predictedScore"))

    // Debug: Controlla quante raccomandazioni ci sono per l'utente target
    println(s"Numero di raccomandazioni per l'utente target: ${recommendationsForUser.count()}")

    // ** Normalizza i punteggi da 0 a 10 **
    val scoreStats = recommendationsForUser.agg(
      min("predictedScore").as("minScore"),
      max("predictedScore").as("maxScore")
    ).collect()

    val minScore = scoreStats(0).getAs[Double]("minScore")
    val maxScore = scoreStats(0).getAs[Double]("maxScore")

    // Debug: Verifica i valori di minScore e maxScore
    println(s"Min Score: $minScore, Max Score: $maxScore")

    val recommendationsWithNormalizedScoreDF = if (maxScore != minScore) {
      recommendationsForUser.withColumn(
        "NormalizedScore",
        least(bround(lit(5) * (col("predictedScore") - lit(minScore)) / (lit(maxScore) - lit(minScore)), 2), lit(10))
      )
    } else {
      recommendationsForUser.withColumn("NormalizedScore", lit(5))
    }

    // Filtra i primi N risultati per l'utente target
    val topRecommendationsDF = recommendationsWithNormalizedScoreDF
      .select("movieId1", "NormalizedScore")
      .orderBy(desc("NormalizedScore"))
      .limit(topN)

    // Salva i primi N risultati in un file CSV
    topRecommendationsDF.write
      .option("header", "true")
      .csv(csvOutputPath)

    spark.stop()
  }
}
