package CollaborativeItemModule

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object CollaborativeFiltering {
  def main(args: Array[String]): Unit = {
    // Crea la SparkSession
    val spark = SparkSession.builder()
      .appName("CollaborativeFiltering")
      .master("local[*]") // Usa tutti i core disponibili
      .getOrCreate()

    // Carica i dati dei film e delle raccomandazioni
    val moviesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("movies.csv")  // Adatta il percorso del tuo file
      .select("movieId", "userId", "rating")

    val recommendationsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("recommendations.csv") // Adatta il percorso del tuo file
      .select("movieId", "recommendationScore")

    // Mostra i dati per il debug
    moviesDF.show()
    recommendationsDF.show()

    // ** Crea una matrice utente-film (user-item matrix) **
    val userMovieRatings = moviesDF
      .groupBy("userId", "movieId")
      .agg(avg("rating").as("rating"))
      .na.fill(0)  // Riempie i valori nulli con 0

    val userMovieMatrix = userMovieRatings
      .groupBy("userId")
      .pivot("movieId")
      .agg(first("rating"))
      .na.fill(0)  // Riempie i valori nulli con 0

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

    import spark.implicits._
    // Fai il join con il DataFrame delle raccomandazioni
    val predictionDF = recommendationsWithAliasDF1
      .join(recommendationsWithAliasDF2, $"df1.movieId2" === $"df2.movieId1", "inner")
      .groupBy("df1.movieId1")
      .agg(sum($"df1.similarity" * $"df2.similarity").as("predictedScore"))
      .orderBy(desc("predictedScore"))

    // ** Normalizza i punteggi da 0 a 10 **
    // Trova il minimo e il massimo dei predictedScore
    val scoreStats = predictionDF.agg(
      min("predictedScore").as("minScore"),
      max("predictedScore").as("maxScore")
    ).collect()

    val minScore = scoreStats(0).getAs[Double]("minScore")
    val maxScore = scoreStats(0).getAs[Double]("maxScore")

    // Aggiungi una colonna NormalizedScore arrotondata e limitata a 10
    val predictionWithNormalizedScoreDF = if (maxScore != minScore) {
      predictionDF.withColumn(
        "NormalizedScore",
        least(bround(lit(10) * (col("predictedScore") - lit(minScore)) / (lit(maxScore) - lit(minScore)), 2), lit(10))
      )
    } else {
      // Assegna un valore fisso (es. 5) in caso di punteggi costanti
      predictionDF.withColumn("NormalizedScore", lit(5))
    }

    // Visualizza le raccomandazioni normalizzate
    predictionWithNormalizedScoreDF.select("movieId1", "NormalizedScore").show()

    // ** Salvataggio dei risultati in un file CSV **
    predictionWithNormalizedScoreDF
      .select("movieId1", "NormalizedScore") // Seleziona solo le colonne finali
      .write
      .option("header", "true")
      .csv("normalized_predicted_recommendations.csv")

    spark.stop()
  }
}

