package SentimentAnalysisModule
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}
import java.nio.channels.Channels
 
object SentimentCSVProcessorSpark {
  def main(args: Array[String]): Unit = {
    val sentimentInputPath = "gs://your-bucket/input/user_reviews_final_sampled.csv"
    val sentimentOutputPath = "gs://your-bucket/output/user_reviews_with_sentiment.csv"
    processCSV(sentimentInputPath, sentimentOutputPath)
  }
 
  def processCSV(sentimentInputPath: String, sentimentOutputPath: String): Unit = {
    //import spark.implicits._

    val spark = SparkSession.builder()
      .appName("Sentiment Analysis")
      .master("local[*]")
      .getOrCreate()
 
    val rawRDD: RDD[String] = spark.sparkContext.textFile(sentimentInputPath)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(row => row != header)

    val parsedRDD: RDD[(String, String, String, String, String)] = dataRDD.mapPartitions(rows => {
      rows.map(row => {
        val cols = row.split(",")
        (cols(0), cols(1), cols(2), cols(3), cols(4)) // (movieId, rating, quote, creationDate, userId)
      })
    })
 
    // Broadcast the PretrainedPipeline model
    // val pipelineSentiment = PretrainedPipeline("analyze_sentiment", lang = "en")
    // val broadcastPipeline = spark.sparkContext.broadcast(pipelineSentiment)
 
    def sentimentToScore(sentiment: String): Double = {
      sentiment match {
        case "positive" => 4.0 + scala.util.Random.nextDouble()
        case "neutral"  => 2.0 + scala.util.Random.nextDouble()
        case "negative" => 0.0 + scala.util.Random.nextDouble()
        case _           => 2.5
      }
    }
 
    val sentimentRDD = parsedRDD.mapPartitions(rows => {
      if (rows.isEmpty) {
        Iterator.empty // Gestisce partizioni vuote
      } else {
        val pipelineSentiment = PretrainedPipeline("analyze_sentiment", lang = "en")
        val broadcastPipeline = spark.sparkContext.broadcast(pipelineSentiment)
        rows.map { case (movieId, rating, quote, creationDate, userId) =>
          val sentiment = broadcastPipeline.value
            .annotate(quote)
            .getOrElse("sentiment", Seq("neutral")).head
          val sentimentScore = sentimentToScore(sentiment).formatted("%.2f").toDouble
          (movieId, rating, sentimentScore, creationDate, userId)
        }
      }
    })
    
    println(s"Total records in sentimentRDD: ${sentimentRDD.count()}")
 
    val headerOutput = "movieId,rating,sentimentResult,creationDate,userId"
    val resultsWithHeader = spark.sparkContext.parallelize(Seq(headerOutput)) ++ sentimentRDD.map {
      case (movieId, rating, sentimentScore, creationDate, userId) =>
        s"$movieId,$rating,$sentimentScore,$creationDate,$userId"
    }
 
    saveSentimentToGcs(resultsWithHeader, sentimentOutputPath)
    spark.stop()
  }
 
  def saveSentimentToGcs(resultsWithHeader: RDD[String], outputPath: String): Unit = {
    resultsWithHeader
      .coalesce(1)
      .foreachPartition(partition => {
        val storage: Storage = StorageOptions.getDefaultInstance.getService
        val uri = new java.net.URI(outputPath)
        val bucketName = uri.getHost
        val objectName = uri.getPath.stripPrefix("/")
        val blobInfo = BlobInfo.newBuilder(bucketName, objectName).build()
        val gcsWriter = Channels.newOutputStream(storage.writer(blobInfo))
  
      val writer = new java.io.PrintWriter(gcsWriter)

      partition.foreach(writer.println)
      writer.close()
    })

    println(s"Results saved to $outputPath")
  }

}

 