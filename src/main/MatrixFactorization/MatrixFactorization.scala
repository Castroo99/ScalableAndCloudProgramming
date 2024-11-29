import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types._
import com.github.tototoshi.csv._

object MatrixFactorization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ALSMatrixFactorization")
      .master("local[*]") // esecuzione in locale
      .getOrCreate()

    val inputFile = "../../processed/user_reviews_with_sentiment.csv"
    val outputFile = "../../processed/user_reviews_factorized.csv"

    val schema = StructType(Array(
      StructField("rating", DoubleType, true),  
      StructField("creationDate", StringType, true),
      StructField("sentimentResult", DoubleType, true), 
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true)
    ))

    val df = spark.read.option("header", "true")
            .schema(schema)
            .csv(inputFile)


    val dfCast = df
      .withColumn("rating", col("rating").cast("double"))
      .withColumn("sentimentResult", col("sentimentResult").cast("double"))
      .withColumn("userId", col("userId").cast("int"))
      .withColumn("movieId", col("movieId").cast("int"))
      
    // calcolo del punteggio finale combinando rating e sentimentResult
    val dfWeighted = dfCast.withColumn(
      "totalScore",
      col("rating") * lit(0.5) + col("sentimentResult") * lit(0.5)
    )

    dfWeighted.printSchema()
    dfWeighted.show()

    val Array(trainingData, testData) = dfWeighted.randomSplit(Array(0.8, 0.2))
    
    val als = new ALS()
      .setMaxIter(10) 
      .setRegParam(0.1) 
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("totalScore")

    val model = als.fit(trainingData)
    
    // Valutazione modello ALS
    // val predictions = model.transform(testData)

    // val evaluator = new RegressionEvaluator()
    //   .setMetricName("rmse")
    //   .setLabelCol("totalScore")
    //   .setPredictionCol("prediction")

    // val rmse = evaluator.evaluate(predictions)
    // println(s"Root-mean-square error = $rmse")

    // predictions.select("userId", "movieId", "prediction").show()
    
    // per ogni utente, consiglia i top 5 movieId 
    val userRecs = model.recommendForAllUsers(5)
    userRecs.show(false)

    saveRecommendationsToCsv(userRecs, outputFile)
    
    // // per ogni film, consiglia i top 5 userId
    // val movieRecs = model.recommendForAllItems(5)
    // movieRecs.show(false) 

    spark.stop()
  }

  def saveRecommendationsToCsv(userRecs: DataFrame, outputPath: String): Unit = {
    val explodedRecs = userRecs.withColumn("recommendations", explode(col("recommendations")))
      .select(
        col("userId"),
        col("recommendations.movieId").alias("movieId"),
        col("recommendations.rating").alias("totalScore")
      )

    val writer = CSVWriter.open(new java.io.File(outputPath))
    writer.writeRow(Seq("userId", "movieId", "totalScore"))
    
    writer.writeAll(explodedRecs.collect().map(row => 
      Seq(row.getAs[Int]("userId").toString, 
          row.getAs[Int]("movieId").toString, 
          row.getAs[Double]("totalScore").toString)
    ))
  }
}
