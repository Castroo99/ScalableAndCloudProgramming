import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import scala.util.Random
import com.github.tototoshi.csv._

object MatrixFactorizationRDD {
  def main(args: Array[String]): Unit = {

    // var bucketName = "recommendation-system-lfag"
	  // var inputFile = "processed-dataset/user_reviews_with_sentiment.csv"
    // var outputFile = "processed-dataset/user_reviews_factorizedRDD.csv"

    // val basePath = s"gs://$bucketName"
	  // val datasetPath = s"$basePath/$inputFile"
	  // val outputPath = s"$basePath/$outputFile"

    val inputFile = "../processed/user_reviews_with_sentiment.csv"
    val outputFile = "../processed/user_reviews_factorizedRDD.csv"
    
    val sparkSession = SparkSession.builder()
        .appName("MatrixFactorizationRDD")
        .master("local[*]")
        .getOrCreate()

    val spark = sparkSession.sparkContext

    val rawRdd: RDD[String] = spark.textFile(inputFile)
    
    // header rimosso da RDD
    val header = rawRdd.first()
    val dataRdd: RDD[String] = rawRdd.filter(line => line != header)
    
    // mappa ogni riga del csv in un oggetto Rating con userId, movieId e totalScore
    val ratingsRdd: RDD[Rating] = dataRdd.map { line =>
      val fields = line.split(",")
      val userId = fields(3).toInt
      val movieId = fields(4).toInt
      val rating = fields(0).toDouble
      val sentimentResult = fields(2).toDouble
      val totalScore = (rating * 0.5) + (sentimentResult * 0.5)
      Rating(userId, movieId, totalScore)
    }

    //TODO ripristinare userId e movieId originali (inserire collect a ogni map(lazy))
    // // estrazione ID originali di utenti e film
    // val userIds: Seq[Int] = ratingsRdd.map(_.user).distinct().collect().toSeq
    // val movieIds: Seq[Int] = ratingsRdd.map(_.product).distinct().collect().toSeq

    // // mappa da userId/movieId a indice numerico
    // val userIdToIndex = userIds.zipWithIndex.toMap
    // val movieIdToIndex = movieIds.zipWithIndex.toMap

    // // mappa da indice a userId e movieId (per ripristinare id nel csv di output)
    // val indexToUserId = userIdToIndex.map(_.swap)
    // val indexToMovieId = movieIdToIndex.map(_.swap)


    // configurazione dei parametri della matrice
    val numUsers = ratingsRdd.map(_.user).distinct().count().toInt    
    val numMovies = ratingsRdd.map(_.product).distinct().count().toInt
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
    var U_rdd = spark.parallelize(U.zipWithIndex.map { case (row, idx) => (idx, row) })
    var M_rdd = spark.parallelize(M.zipWithIndex.map { case (row, idx) => (idx, row) })

    // // Salva le matrici iniziali
    // saveRddToCsv(U_rdd, "U_rdd_initial.csv")
    // saveRddToCsv(M_rdd, "M_rdd_initial.csv")

    // modello addestrato con discesa del gradiente
    for (iteration <- 1 to numIterations) {
      // println(s"Iterazione: $iteration")

      // ogni oggetto Rating mappato con la previsione sul rating e l'errore
      val gradients = ratingsRdd.map { case Rating(userId, movieId, rating) =>
        // // estrazione vettori delle matrici, a partire da map
        // val uIndex = userIdToIndex(userId)
        // val mIndex = movieIdToIndex(movieId)

        // Estrai i vettori U e M usando gli indici
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

      // RDD aggiornate con i gradienti calcolati in modo distribuito sottraendo alpha
    //   U_rdd = U_rdd.join(updated_U).mapValues { case (u, uGrad) =>
    //     u.zip(uGrad).map { case (uVal, uGradVal) => uVal - alpha * uGradVal }
    //   }
    //   M_rdd = M_rdd.join(updated_M).mapValues { case (m, mGrad) =>
    //     m.zip(mGrad).map { case (mVal, mGradVal) => mVal - alpha * mGradVal }
    //   }

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
      (userId, movieId, ratingPrediction) //RDD generato, con ratingPrediction come score predetto
    }

    // // collect() costringe all'esecuzione cartesian() (lazy)
    // val userRecommendationsCollected = userRecommendations.collect()
    
    // estrazione di 5 top film per ogni utente
    val top5Recs = userRecommendations
        .groupBy(_._1)  // per userId
        .mapValues(recs => recs.toList.sortBy(-_._3).take(5)) //rating decrescente, top 5

    val recommendations = top5Recs.flatMap { case (userId, recs) =>
      recs.filter { case (_, _, rating) => rating > 0.0 }  // estratte solo recs con rating > 0
      .map { case (_, movieId, rating) => (userId, movieId, rating) }
    }

    // val recommendationsWithOriginalIds = recommendations.map { case (userIndex, movieIndex, rating) =>
    //   (indexToUserId(userIndex), indexToMovieId(movieIndex), rating)
    // }
    
//     saveRddToCsv(
//     userRecommendations,
//     "userRecommendations.csv",
//     Seq("userId", "movieId", "ratingPrediction")
// )

    val recommendationsRdd = spark.parallelize(recommendations.collect()) 
    saveRecommendationsToCsv(recommendationsRdd, outputFile)

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

//   def saveRddToCsv[T](rdd: RDD[T], outputPath: String, header: Seq[String]): Unit = {
//     val writer = CSVWriter.open(new java.io.File(outputPath))
//     writer.writeRow(header) // Scrivi l'header
    
//     // Scrivi tutte le righe del file
//     rdd.collect().foreach { row =>
//         writer.writeRow(row match {
//             case product: Product => product.productIterator.map(_.toString).toSeq
//             case other => Seq(other.toString)
//         })
//     }
    
//     writer.close()
//    }    

}
