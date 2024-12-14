package MatrixFactorizationALSPackage

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import com.github.tototoshi.csv._
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import java.nio.channels.Channels
import java.io.ByteArrayOutputStream
// import kantan.csv._
// import kantan.csv.ops._
import scala.math.BigDecimal.RoundingMode
import org.apache.spark.mllib.evaluation.RegressionMetrics


object MatrixFactorizationRDD_ALS {
  //❌💪
  def main(args: Array[String]): Unit = {
    // if (args.length < 4) {
    //   println("Usage: MatrixFactorizationRDD_ALS <bucketName> <sentimentFile> <outputFile>")
    //   System.exit(1)
    // }

    // val targetUser = args(0).toInt
    // val topN = args(1).toInt
    // val sentimentDF = args(2)
    // val outputFile = args(3)
    // matrixFactorizationRDDAls(targetUser, topN, sentimentFile, outputFile)
  // }

  // def matrixFactorizationRDDAls(targetUser: Int, topN: Int, sentimentFile: String, outputFile: String): Unit = {
  //   print("Starting MatrixFactorizationRDD_ALS")
    
    val bucketName = "recommendation-system-lfag"
    val basePath = s"gs://$bucketName"
    val targetUser = 447145//args(0).toInt
    val topN = 20 //args(1).toInt
    val sentimentFile = f"${basePath}/processed-dataset/user_reviews_with_sentiment.csv"//args(2)
    val outputFile = f"${basePath}/processed-dataset/matrix_factorization_RDD.csv"//args(3)
    // val sentimentFile = "../processed/new_df_sentiment.csv"//args(2)
    // val outputFile = "../processed/matrixFactRddALS_output.csv"//args(3)
  }
  
  def matrixFactorizationRDDAls(spark: SparkSession, userId_selected: Int, numMoviesRec: Int, sentimentFile: String, outputFile: String): Unit = {
    print("Starting MatrixFactorizationRDD_ALS")
    import spark.implicits._
    val startTime = System.nanoTime()
    val rawRdd: RDD[String] = spark.sparkContext.textFile(sentimentFile)
    
    // header rimosso da RDD
    val header = rawRdd.first()
    val dataRdd: RDD[String] = rawRdd.filter(line => line != header)
    
    // mappa ogni riga del csv in un oggetto Rating con userId, movieId e totalScore
    val ratingsRdd: RDD[Rating] = dataRdd.map { 
      line =>
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
    }

    // set di film già visti dall'utente selezionato
    val movies_watched: Set[Int] = ratingsRdd.filter(_.user == targetUser)
      .map(_.product).collect().toSet
    
    val Array(trainingRdd, testRdd) = ratingsRdd.randomSplit(Array(0.8, 0.2))

    val rank = 10
    val numIterations = 10
    val lambda = 0.1

    val model = ALS.train(trainingRdd, rank, numIterations, lambda)    
    val predictions: RDD[Rating] = model.predict(testRdd.map(r => (r.user, r.product)))

    val predictionsMap = predictions
      .map(r => ((r.user, r.product), r.rating))
      .collectAsMap()

    val predictionsAndLabels = testRdd.map { case Rating(userId, movieId, rating) =>
      val predictedRating = predictionsMap.getOrElse((userId, movieId), 0.0)
      (predictedRating, rating)
    }

    val metrics = new RegressionMetrics(predictionsAndLabels)
    val rmse = metrics.rootMeanSquaredError
    val mae = metrics.meanAbsoluteError
    println(s"RMSE: $rmse")
    println(s"MAE: $mae")

    // generarazione di topN film raccomandati per ogni utente
    val userRecs: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(topN)
    val filteredRecs: RDD[(Int, Array[Rating])] = userRecs.filter {
      case (userId, _) => userId == targetUser
    }.map { case (userId, recs) =>
      // rimossi film già visti
      (userId, recs.filterNot(r => movies_watched.contains(r.product)).map { r =>
        val roundedRating = BigDecimal(r.rating).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        Rating(r.user, r.product, roundedRating)
      })
    }

    saveRecommendationsToGcs(filteredRecs, outputFile)
    print("End MatrixFactorizationRDD_ALS")

    val endTime = System.nanoTime()
    // Calcola e stampa il tempo di esecuzione
    val duration = (endTime - startTime) / 1e9d // In secondi
    println(s"Tempo di esecuzione: $duration secondi")
  }
  def saveRecommendationsToCsv(userRecs: RDD[(Int, Array[Rating])], outputPath: String): Unit = {
    print("Starting saveRecommendationsToCsv")
    val recommendations: RDD[(Int, Int, Double)] = userRecs.flatMap {
      // case (userId, recs) => recs.map(r => (userId, r.product, r.rating))
      case (userId, recs) => recs.map(r => (userId, r.product, 
        BigDecimal(r.rating).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble))
    }

    val writer = CSVWriter.open(new java.io.File(outputPath))
    writer.writeRow(Seq("userId", "movieId", "totalScore"))
    
    writer.writeAll(recommendations.collect().map {
      case (userId, movieId, totalScore) =>
        Seq(userId.toString, movieId.toString, totalScore.toString)
    })
    print("Ending saveRecommendationsToCsv")
  }

  def saveRecommendationsToGcs(userRecs: RDD[(Int, Array[Rating])], outputPath: String): Unit = {
    print("Starting MatrixFactorizationRDD_ALS.saveRecommendationsToGcs")
    val recommendations: RDD[(Int, Int, Double)] = userRecs.flatMap {
      // case (userId, recs) => recs.map(r => (userId, r.product, r.rating))
      case (userId, recs) => recs.map(r => (userId, r.product, 
        BigDecimal(r.rating).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble))
    }

    // Convert recommendations to CSV format in memory
    val csvData = new ByteArrayOutputStream()
    val writer = CSVWriter.open(csvData)

    writer.writeRow(Seq("userId", "movieId", "totalScore"))

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
