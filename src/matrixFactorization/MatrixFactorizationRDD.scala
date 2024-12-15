package  MatrixFactorizationALSPackage

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
import org.apache.spark.storage.StorageLevel

object MatrixFactorizationRDD {
  def main(args: Array[String]): Unit = {

    // if (args.length < 5) {
    //   println("Usage: MatrixFactorizationRDD <bucketName> <sentimentFile> <outputFile>")
    //   System.exit(1)
    // }
//     matrixFactorizationRdds(targetUser, topN, sentimentFile, outputFile)
//     }

//    def matrixFactorizationRdd(targetUser: Int, topN: Int, sentimentFile: DataFrame, outputFile: String): Unit = {
//     print("Starting MatrixFactorizationRDD")
    val bucketName = "recommendation-system-lfag"
    val basePath = s"gs://$bucketName"
    val targetUser = 447145//args(0).toInt 
    val topN = 20 //args(1).toInt
    val sentimentFile = f"${basePath}/processed-dataset/user_reviews_with_sentiment.csv"//args(2)
    val outputFile = f"${basePath}/processed-dataset/matrix_factorization_RDD.csv"//args(3)
    // val sentimentFile = "../processed/new_df_sentiment.csv"//args(2)
    // val outputFile = "../processed/matrixFactRddALS_output.csv"//args(3)

    val startTime = System.nanoTime()

    val spark: SparkSession = SparkSession.builder()
    .appName("MatrixFactorizationRDD")
    .master("local[*]")
    .config("spark.executor.memory", "8g") // Imposta la memoria per ciascun executor
    .config("spark.driver.memory", "8g") // Imposta la memoria per il driver 
    .config("spark.executor.memoryOverhead", "2g")
    .config("spark.driver.maxResultSize", "4g")  // Limita la dimensione dei risultati restituiti
    .config("spark.storage.memoryFraction", "0.4")
    .getOrCreate()

    val rawRdd: RDD[String] = spark.sparkContext.textFile(sentimentFile)

    // header rimosso da RDD
    val header = rawRdd.first()
    val dataRdd: RDD[String] = rawRdd.filter(line => line != header)
    
    val ratingsRdd: RDD[Rating] = dataRdd.map {line =>
        val fields = line.split(",")
        val userId = fields(2).toInt
        val movieId = fields(0).toInt
        val rating = fields(1).toDouble
        val sentimentResult = fields(3).toDouble
        val totalScore = {
            val normalizedRating = math.min(math.max(rating, 0), 5)
            val normalizedSentiment = math.min(math.max(sentimentResult, 0), 5)
            (normalizedRating * 0.5) + (normalizedSentiment * 0.5)
        }
      Rating(userId, movieId, totalScore)
    }.repartition(200)

    ratingsRdd.persist(StorageLevel.MEMORY_AND_DISK)

    // configurazione dei parametri della matrice
    val numUsers = ratingsRdd.map(_.user).distinct().count()
    val numMovies = ratingsRdd.map(_.product).distinct().count()
    val rank = 10             // num feature latenti
    val numIterations = 10    // iterazioni di discesa del gradiente
    val lambda = 0.1          // regolarizzazione
    val alpha = 0.01          // tasso apprendimento

    // creazione matrici Users e Movies, inizializzate casualmente
    val U = initializeMatrix(numUsers.toInt, rank)
    val M = initializeMatrix(numMovies.toInt, rank)

    // matrici convertite da array bidimensionali in map
    val U_map = U.zipWithIndex.map { case (row, idx) => (idx, row) }.toMap
    val M_map = M.zipWithIndex.map { case (row, idx) => (idx, row) }.toMap
    val U_map_bc = spark.sparkContext.broadcast(U_map)
    val M_map_bc = spark.sparkContext.broadcast(M_map)

    // matrici distribuite tra i nodi del cluster
    // ogni riga mappata con un indice, creando un RDD
    var U_rdd = spark.sparkContext.parallelize(ratingsRdd.map(_.user).distinct().collect().map { userId =>
    (userId, Array.fill(rank)(scala.util.Random.nextDouble())) // inizializzata casualmente
    })

    var M_rdd = spark.sparkContext.parallelize(ratingsRdd.map(_.product).distinct().collect().map { movieId =>
    (movieId, Array.fill(rank)(scala.util.Random.nextDouble())) 
    })

    U_rdd = U_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    M_rdd = M_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    // modello addestrato con discesa del gradiente
    for (iteration <- 1 to numIterations) {
      // println(s"Iterazione: $iteration")

      // ogni oggetto Rating mappato con la previsione sul rating e l'errore
      val gradients = ratingsRdd.map { case Rating(userId, movieId, rating) =>
        // estrazione vettori delle matrici U e M
        val u = U_map_bc.value.getOrElse(userId, Array.fill(rank)(0.0))
        val m = M_map_bc.value.getOrElse(movieId, Array.fill(rank)(0.0))

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

        U_rdd = U_rdd.join(updated_U).mapValues {
            case (u, uGrad) => u.zip(uGrad).map { case (uVal, uGradVal) => uVal - alpha * uGradVal }
        }

        M_rdd = M_rdd.join(updated_M).mapValues {
            case (m, mGrad) => m.zip(mGrad).map { case (mVal, mGradVal) => mVal - alpha * mGradVal }
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
      val limitedRating = math.min(math.max(ratingPrediction, 0), 5)
      (userId, movieId, limitedRating) 
    } 

    // estrazione dei primi 5 film raccomandati per ogni utente
    val top5Recs = userRecommendations
      .groupBy(_._1)  // per userId
      .mapValues(recs => recs
          .groupBy(_._2)  // per movieId
          .mapValues(_.head) // rimossi duplicati per movieId
          .values.toList   
          .sortBy(-_._3)   // score decrescente
          .take(topN))        // primi 5 film


    val recommendations = top5Recs.flatMap { case (userId, recs) =>
        recs.filter { case (_, _, rating) => rating > 0.0 }  // estratti solo rating > 0
            .flatMap { case (_, movieId, rating) =>
                val formattedRating = BigDecimal(rating).setScale(2, RoundingMode.HALF_UP).toDouble
                Some((userId, movieId, formattedRating))
            }
    }
    // val recommendationsRdd = spark.sparkContext.parallelize(recommendations.collect()) 

    val filteredRecs: RDD[(Int, Int, Double)] = recommendations
      .filter { case (userId, _, _) => userId == targetUser }

    saveRecommendationsToGcs(filteredRecs, outputFile)

    val endTime = System.nanoTime()
    // Calcola e stampa il tempo di esecuzione
    val duration = (endTime - startTime) / 1e9d // In secondi
    println(s"Tempo di esecuzione: $duration secondi")

    // spark.stop()
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
  
  def saveRecommendationsToGcs(recommendations: RDD[(Int, Int, Double)], outputPath: String): Unit = {
    print("Starting MatrixFactorizationRDD.saveRecommendationsToGcs")

    // Convert recommendations to CSV format in memory
    val csvData = new ByteArrayOutputStream()
    val writer = CSVWriter.open(csvData)

    // Scrive header
    writer.writeRow(Seq("userId", "movieId", "totalScore"))

    // Scrive i dati
    writer.writeAll(
      recommendations.collect().map {
        case (userId, movieId, totalScore) => Seq(userId.toString, movieId.toString, totalScore.toString)
      }
    )

    writer.close()

    // Configurazione e salvataggio su GCS
    val storage: Storage = StorageOptions.getDefaultInstance.getService
    val uri = new java.net.URI(outputPath)
    val bucketName = uri.getHost
    val objectName = uri.getPath.stripPrefix("/")

    val blobInfo = BlobInfo.newBuilder(bucketName, objectName).build()
    val gcsWriter = Channels.newOutputStream(storage.writer(blobInfo))
    gcsWriter.write(csvData.toByteArray)
    gcsWriter.close()

    println(s"Recommendations saved to $outputPath")
  }
}
