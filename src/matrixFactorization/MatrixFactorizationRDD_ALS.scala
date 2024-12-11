package MatrixFactorizationModule

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.{SparkConf, SparkContext} 
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import com.github.tototoshi.csv._
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import java.nio.channels.Channels
import java.io.ByteArrayOutputStream
import kantan.csv._
import kantan.csv.ops._
import scala.math.BigDecimal.RoundingMode


object MatrixFactorizationRDD_ALS {
  //❌💪
  def main(args: Array[String]): Unit = {
    // if (args.length < 4) {
    //   println("Usage: MatrixFactorizationRDD_ALS <bucketName> <sentimentFile> <outputFile>")
    //   System.exit(1)
    // }

    val userId_selected = args(0).toInt
    val numMoviesRec = args(1).toInt
    val sentimentDF = args(2)
    val outputFile = args(3)

    print("Starting MatrixFactorizationRDD_ALS")


    // Crea la sessione Spark
    val spark: SparkSession = SparkSession.builder()
      .appName("ReccSys")
      .master("local[4]") // 4 thread
      .getOrCreate()
    
    import spark.implicits._

          
    // val rawRdd: RDD[String] = spark.sparkContext.textFile(sentimentFile)
    
    // // header rimosso da RDD
    // val header = rawRdd.first()
    // val dataRdd: RDD[String] = rawRdd.filter(line => line != header)
    
    // mappa ogni riga del csv in un oggetto Rating con userId, movieId e totalScore
    val ratingsRdd: RDD[Rating] = sentimentDF.rdd.map { 
      row =>
      val userId = row.getAs[Int]("userId")
      val movieId = row.getAs[Int]("movieId")
      val rating = row.getAs[Double]("rating")
      val sentimentResult = row.getAs[Double]("sentimentResult")
      //line =>
      // val fields = line.split(",")
      // val userId = fields(3).toInt
      // val movieId = fields(0).toInt
      // val rating = fields(1).toDouble
      // val sentimentResult = fields(4).toDouble
      val totalScore = (rating * 0.5) + (sentimentResult * 0.5)
      Rating(userId, movieId, totalScore)
    }//.repartition(numPartitions)

    val Array(trainingRdd, testRdd) = ratingsRdd.randomSplit(Array(0.8, 0.2))

    val rank = 10
    val numIterations = 10
    val lambda = 0.1

    val model = ALS.train(trainingRdd, rank, numIterations, lambda)

    // generarazione di 5 film raccomandati per ogni utente
    val userRecs: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(numMoviesRec)

    // recs filtrate per utente selezionato
    val filteredRecs: RDD[(Int, Array[Rating])] = userRecs.filter {
      case (userId, _) => userId == userId_selected
    }.map {case (userId, recs) => (userId, recs.map { r =>
          val roundedRating = BigDecimal(r.rating).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          Rating(r.user, r.product, roundedRating)
      })
    }

    saveRecommendationsToGcs(filteredRecs, outputFile)

    print("End MatrixFactorizationRDD_ALS")
  }

  // def saveRecommendationsToCsv(userRecs: RDD[(Int, Array[Rating])], outputPath: String): Unit = {
  //   val recommendations: RDD[(Int, Int, Double)] = userRecs.flatMap {
  //     case (userId, recs) => recs.map(r => (userId, r.product, r.rating))
  //   }

  //   val writer = CSVWriter.open(new java.io.File(outputPath))
  //   writer.writeRow(Seq("userId", "movieId", "totalScore"))
    
  //   writer.writeAll(recommendations.collect().map {
  //     case (userId, movieId, totalScore) =>
  //       Seq(userId.toString, movieId.toString, totalScore.toString)
  //   })
  // }

  def saveRecommendationsToGcs(userRecs: RDD[(Int, Array[Rating])], outputPath: String): Unit = {
    print("Starting MatrixFactorizationRDD_ALS.saveRecommendationsToGcs")
    val recommendations: RDD[(Int, Int, Double)] = userRecs.flatMap {
      case (userId, recs) => recs.map(r => (userId, r.product, r.rating))
    }

    // Convert recommendations to CSV format in memory
    val csvData = new ByteArrayOutputStream()
    val writer = CSVWriter.open(csvData)

    // val writer = CSVWriter.open(new java.io.File(outputPath))

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
