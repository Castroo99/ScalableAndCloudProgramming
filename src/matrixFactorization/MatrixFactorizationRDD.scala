package  MatrixFactorizationModule

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import scala.util.Random
import com.github.tototoshi.csv._
import scala.math.BigDecimal.RoundingMode
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import java.nio.channels.Channels
import java.io.ByteArrayOutputStream

object MatrixFactorizationRDD {
  def main(args: Array[String]): Unit = {

    // if (args.length < 5) {
    //   println("Usage: MatrixFactorizationRDD <bucketName> <sentimentFile> <outputFile>")
    //   System.exit(1)
    // }
    matrixFactorizationRdds(userId_selected, numMoviesRec, sentimentDF, outputFile)
    }

   def matrixFactorizationRdd(userId_selected: Int, numMoviesRec: Int, sentimentDF: DataFrame, outputFile: String): Unit = {
    print("Starting MatrixFactorizationRDD")

    val spark: SparkSession = SparkSession.builder()
    .appName("MatrixFactorizationRDD")
    .master("local[4]")
    .getOrCreate()

    // val rawRdd: RDD[String] = spark.sparkContext.textFile(sentimentFile)

    // // header rimosso da RDD
    // val header = rawRdd.first()
    // val dataRdd: RDD[String] = rawRdd.filter(line => line != header)
    
    // // mapping userId-index per salvataggio userId originali
    // val userIdToIndex = inputDf.rdd.map { line =>
    //   val fields = line.split(",")
    //   fields(3).toInt
    // }.distinct().zipWithIndex().collect().toMap
    // val indexToUserId = userIdToIndex.map(_.swap)
    
    // // mapping movieId-index per salvataggio movieId originali
    // val movieIdToIndex = dataRdd.map { line =>
    //   val fields = line.split(",")
    //   fields(0).toInt
    // }.distinct().zipWithIndex().collect().toMap
    // val indexToMovieId = movieIdToIndex.map(_.swap)

    // mapping userId-index per salvataggio userId originali
    val userIdToIndex = sentimentDF
    .select("userId")
    .distinct()
    .rdd
    .map(row => row.getInt(0)) 
    .zipWithIndex()
    .collect()
    .toMap
    
    val indexToUserId = userIdToIndex.map(_.swap)

    // mapping movieId-index per salvataggio movieId originali
    val movieIdToIndex = sentimentDF
      .select("movieId")
      .distinct()
      .rdd
      .map(row => row.getInt(0))
      .zipWithIndex()
      .collect()
      .toMap

    val indexToMovieId = movieIdToIndex.map(_.swap)

    // mappa ogni riga del csv in un oggetto Rating con userId, movieId e totalScore
    val ratingsRdd: RDD[Rating] = sentimentDF.rdd.map { row =>
      val userId = userIdToIndex(row.getInt(row.fieldIndex("userId"))).toInt
      val movieId = movieIdToIndex(row.getInt(row.fieldIndex("movieId"))).toInt
      val rating = row.getDouble(row.fieldIndex("rating"))
      val sentimentResult = row.getDouble(row.fieldIndex("sentimentResult"))
    // line =>
    //   val fields = line.split(",")
    //   val userId = userIdToIndex(fields(3).toInt).toInt
    //   val movieId = movieIdToIndex(fields(0).toInt).toInt
    //   val rating = fields(1).toDouble
    //   val sentimentResult = fields(4).toDouble
      val totalScore = (rating * 0.5) + (sentimentResult * 0.5)
      Rating(userId, movieId, totalScore)
    }

    // configurazione dei parametri della matrice
    val numUsers = userIdToIndex.size
    val numMovies = movieIdToIndex.size
    val rank = 10             // num feature latenti
    val numIterations = 10    // iterazioni di discesa del gradiente
    val lambda = 0.1          // regolarizzazione
    val alpha = 0.01          // tasso apprendimento

    // creazione matrici Users e Movies, inizializzate casualmente
    val U = initializeMatrix(numUsers, rank)
    val M = initializeMatrix(numMovies, rank)

    // matrici convertite da array bidim in map
    val U_map = U.zipWithIndex.map { case (row, idx) => (idx, row) }.toMap
    val M_map = M.zipWithIndex.map { case (row, idx) => (idx, row) }.toMap
    
    // matrici distribuite tra i nodi del cluster
    // ogni riga mappata con un indice, creando un RDD
    var U_rdd = spark.sparkContext.parallelize(U.zipWithIndex.map { case (row, idx) => (idx, row) })
    var M_rdd = spark.sparkContext.parallelize(M.zipWithIndex.map { case (row, idx) => (idx, row) })

    // modello addestrato con discesa del gradiente
    for (iteration <- 1 to numIterations) {
      // println(s"Iterazione: $iteration")

      // ogni oggetto Rating mappato con la previsione sul rating e l'errore
      val gradients = ratingsRdd.map { case Rating(userId, movieId, rating) =>
        // estrazione vettori delle matrici
        val u = U_map.getOrElse(userId, Array.fill(rank)(0.0))
        val m = M_map.getOrElse(movieId, Array.fill(rank)(0.0))
    
        // prodotto fra ogni userId e movieId  
        val prediction = u.zip(m).map { case (uFactor, mFactor) => uFactor * mFactor }.sum
        val error = rating - prediction

        // gradiente di discesa per ogni matrice
        val userGradient = m.zip(u).map { case (mFactor, uFactor) => -2 * error * mFactor + 2 * lambda * uFactor }
        val movieGradient = u.zip(m).map { case (uFactor, mFactor) => -2 * error * uFactor + 2 * lambda * mFactor }

        // creazione coppie per ogni utente e film, usate per aggiornare U_rdd e M_rdd globali
        ((userId, userGradient), (movieId, movieGradient))
      }

      // righe delle matrici aggiornate con i gradienti salvati nelle coppie precedenti
      val updated_U = gradients.map { case ((userId, uGrad), _) => (userId, uGrad) }
      val updated_M = gradients.map { case ((_, _), (movieId, mGrad)) => (movieId, mGrad) }

      U_rdd = U_rdd.fullOuterJoin(updated_U).mapValues {
          case (Some(u), Some(uGrad)) => u.zip(uGrad).map { case (uVal, uGradVal) => uVal - alpha * uGradVal }
          case (Some(u), None) => u                         // se non ci sono aggiornamenti, lascia il vecchio valore
          case (None, Some(uGrad)) => Array.fill(rank)(0.0) // se l'utente non esiste prende il valore iniziale
          case _ => Array.fill(rank)(0.0)
      }
      
      M_rdd = M_rdd.fullOuterJoin(updated_M).mapValues {
      case (Some(m), Some(mGrad)) => m.zip(mGrad).map { case (mVal, mGradVal) => mVal - alpha * mGradVal }
      case (Some(m), None) => m                         // se non ci sono aggiornamenti, lascia il vecchio valore
      case (None, Some(mGrad)) => Array.fill(rank)(0.0) // se il film non esiste prende il valore iniziale
      case _ => Array.fill(rank)(0.0)
      }
        

    //   // calcolo dell'errore da minimizzare
    //   val loss = gradients.map { case ((userId, uGrad), (movieId, mGrad)) =>
    //     val rating = ratingsRdd.filter(r => r.user == userId && r.product == movieId).map(_.rating).collect().head
    //     //val prediction = U(userId).zip(M(movieId)).map { case (u, m) => u * m }.sum
    //     val prediction = U_map.getOrElse(userId, Array.fill(rank)(0.0)).zip(M_map.getOrElse(movieId, Array.fill(rank)(0.0)))
    //         .map { case (u, m) => u * m }.sum
    //     Math.pow(rating - prediction, 2)
    //     }.sum() + lambda * (U.flatten.map(x => x * x).sum + M.flatten.map(x => x * x).sum)

    
     // println(s"Perdita dopo iterazione $iteration: $loss")
    }

    // prodotto tra le matrici per il calcolo di ogni rating, anche per i film non visti da ogni utente
    val userRecommendations = U_rdd.cartesian(M_rdd).map { case ((userId, u), (movieId, m)) =>
      val ratingPrediction = u.zip(m).map { case (uVal, mVal) => uVal * mVal }.sum
      (userId, movieId, ratingPrediction) //RDD con ratingPrediction come score predetto
    }

    // estrazione dei primi 5 film raccomandati per ogni utente
    val top5Recs = userRecommendations
      .groupBy(_._1)  // per userId
      .mapValues(recs => recs
          .groupBy(_._2)  // per movieId
          .mapValues(_.head) // rimossi duplicati per movieId
          .values.toList   
          .sortBy(-_._3)   // score decrescente
          .take(numMoviesRec))        // primi 5 film


    val recommendations = top5Recs.flatMap { case (userIndex, recs) =>
      val originalUserId = indexToUserId(userIndex) // mapping da indice a userId originale
      recs.filter { case (_, _, rating) => rating > 0.0 } // estratte solo recs con rating > 0
          .map { case (_, movieIndex, rating) =>
            val originalMovieId = indexToMovieId(movieIndex) // mapping da indice a movieId originale
            val formattedRating = BigDecimal(rating).setScale(2, RoundingMode.HALF_UP).toDouble
            (originalUserId, originalMovieId, formattedRating)
          }
    }

    val recommendationsRdd = spark.sparkContext.parallelize(recommendations.collect()) 

    val filteredRecs: RDD[(Int, Int, Double)] = recommendationsRdd
      .filter { case (userId, _, _) => userId == userId_selected }

    saveRecommendationsToCsv(filteredRecs, outputFile)

    spark.stop()
  }

  
  // per inizializzare le matrici casualmente
  def initializeMatrix(numRows: Int, numCols: Int): Array[Array[Double]] = {
    Array.fill(numRows, numCols)(Random.nextDouble() * 1)
  }

  def saveRecommendationsToCsv(recommendations: RDD[(Int, Int, Double)], outputPath: String): Unit = {
    val writer = CSVWriter.open(new java.io.File(outputPath))
    writer.writeRow(Seq("userId", "movieId", "totalScore"))
    
    writer.writeAll(recommendations.collect().map {
      case (userId, movieId, totalScore) =>
        Seq(userId.toString, movieId.toString, totalScore.toString)
    })
  }

  
  // def saveRecommendationsToGcs(recommendations: RDD[(Int, Int, Double)], outputPath: String): Unit = {
  //   print("Starting MatrixFactorizationRDD.saveRecommendationsToGcs")

  //   // Convert recommendations to CSV format in memory
  //   val csvData = new ByteArrayOutputStream()
  //   val writer = CSVWriter.open(csvData)

  //   // Scrive header
  //   writer.writeRow(Seq("userId", "movieId", "totalScore"))

  //   // Scrive i dati
  //   writer.writeAll(
  //     recommendations.collect().map {
  //       case (userId, movieId, totalScore) => Seq(userId.toString, movieId.toString, totalScore.toString)
  //     }
  //   )

  //   writer.close()

  //   // Configurazione e salvataggio su GCS
  //   val storage: Storage = StorageOptions.getDefaultInstance.getService
  //   val uri = new java.net.URI(outputPath)
  //   val bucketName = uri.getHost
  //   val objectName = uri.getPath.stripPrefix("/")

  //   val blobInfo = BlobInfo.newBuilder(bucketName, objectName).build()
  //   val gcsWriter = Channels.newOutputStream(storage.writer(blobInfo))
  //   gcsWriter.write(csvData.toByteArray)
  //   gcsWriter.close()

  //   println(s"Recommendations saved to $outputPath")
  // }
}
