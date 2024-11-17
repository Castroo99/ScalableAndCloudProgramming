import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types._

object MatrixFactorization {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ALSExample")
      .master("local[*]") // esecuzione in locale
      .getOrCreate()

    val dataset_path = "../../processed/user_reviews_reduced.csv"

    val schema = StructType(Array(
        StructField("movieId", StringType, true),
        StructField("rating", DoubleType, true),
        StructField("userId", StringType, true)
    ))

    val df = spark.read.option("header", "true")
            .schema(schema)
            .csv(dataset_path)

    df.printSchema()

    // mappa userId su ID numerico
    val userIndexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userIdIndex")
      .fit(df) 

    // mappa 'movieId' su ID numerico
    val movieIndexer = new StringIndexer()
      .setInputCol("movieId")
      .setOutputCol("movieIdIndex")
      .fit(df) 
    

    val Array(trainingData, testData) = df.randomSplit(Array(0.8, 0.2))
    
    val trainingData_indexed = movieIndexer.transform(userIndexer.transform(df))
    val testData_indexed = movieIndexer.transform(userIndexer.transform(df))

    val als = new ALS()
      .setMaxIter(10) 
      .setRegParam(0.1) 
      .setUserCol("userIdIndex")
      .setItemCol("movieIdIndex")
      .setRatingCol("rating")

    val model = als.fit(trainingData_indexed)

    //TODO: conversione movieId e userId a stringhe originali
    // val userId_reconverted = userIndexer.labels.zipWithIndex.map { 
    //   case (label, index) => (index, label)
    // }.toMap

    // val movieId_reconverted = movieIndexer.labels.zipWithIndex.map { 
    //   case (label, index) => (index, label)
    // }.toMap

    // val convertUserId = udf((userIdIndex: Double) => userId_reconverted.getOrElse(userIdIndex.toInt, "Unknown"))
    // val convertMovieId = udf((movieIdIndex: Double) => movieId_reconverted.getOrElse(movieIdIndex.toInt, "Unknown"))

    
    val predictions = model.transform(testData_indexed)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    predictions.select("userIdIndex", "movieIdIndex", "prediction").show()
    
    // per ogni utente, consiglia i top 5 movieId 
    val userRecs = model.recommendForAllUsers(5)
    userRecs.show(false)

    // per ogni film, consiglia i top 5 userId
    val movieRecs = model.recommendForAllItems(5)
    movieRecs.show(false) 

    spark.stop()
  }
}
