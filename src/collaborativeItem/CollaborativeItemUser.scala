package CollaborativeItemModule

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object CollaborativeItemUser {
  def main(args: Array[String]): Unit = {
    var bucketName = "recommendation-system-lfag"
	  var moviesFile = "full-dataset/movies.csv"
	  var reccFile = "processed-dataset/user_reviews_factorized_RDD_ALS.csv"
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

    // Definisci l'utente per il quale vuoi calcolare le raccomandazioni
    val targetUserId = "901245019" // Sostituisci con l'ID dell'utente desiderato

    // Carica i dati dei film e delle raccomandazioni
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(moviesPath)  // Adatta il percorso del tuo file
      .select("movieId", "userId", "rating")

    val recommendationsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(reccPath) // Adatta il percorso del tuo file
      .select("userId", "movieId", "recommendationValue")

    // Filtra i dati per l'utente specifico
    val filteredRecommendationsDF = recommendationsDF.filter($"userId" === targetUserId)

    // Mostra i dati filtrati per il debug
    moviesDF.show()
    filteredRecommendationsDF.show()

    // ** Crea una matrice utente-film (user-item matrix) **
    val userMovieRatings = moviesDF
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

    // Aggiungi alias per evitare conflitti nelle colonne durante il join
    val recommendationsWithAliasDF1 = recommendationsWithSimilarityDF.as("df1")
    val recommendationsWithAliasDF2 = recommendationsWithSimilarityDF.as("df2")

    // Fai il join con il DataFrame delle raccomandazioni
    val predictionDF = recommendationsWithAliasDF1
      .join(recommendationsWithAliasDF2, $"df1.movieId2" === $"df2.movieId1", "inner")
      .groupBy("df1.movieId1")
      .agg(sum($"df1.similarity" * $"df2.similarity").as("predictedScore"))
      .orderBy(desc("predictedScore"))

    // ** Normalizza i punteggi da 0 a 10 **
    val scoreStats = predictionDF.agg(
      min("predictedScore").as("minScore"),
      max("predictedScore").as("maxScore")
    ).collect()

    val minScore = scoreStats(0).getAs[Double]("minScore")
    val maxScore = scoreStats(0).getAs[Double]("maxScore")

    val predictionWithNormalizedScoreDF = if (maxScore != minScore) {
      predictionDF.withColumn(
        "NormalizedScore",
        least(bround(lit(10) * (col("predictedScore") - lit(minScore)) / (lit(maxScore) - lit(minScore)), 2), lit(10))
      )
    } else {
      predictionDF.withColumn("NormalizedScore", lit(5))
    }

    // Visualizza le raccomandazioni normalizzate
    predictionWithNormalizedScoreDF.select("movieId1", "NormalizedScore").show()

    // Salva i risultati
    predictionWithNormalizedScoreDF
      .select("movieId1", "NormalizedScore")
      .write
      .option("header", "true")
      .csv(outputPath)

    spark.stop()
  }
}
