import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollaborativeFilteringDF {

  def main(args: Array[String]): Unit = {
    var targetUser = args(0).toInt
    var topN = args(1).toInt
    var csvInputPath = args(2)
    var csvOutputPath = args(3)

    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Jar Auto partitions")

    var trainName = "movie_reviews_with_random.csv"
    var outputFile = "recommendations.csv"

    if (args.length > 0) {
      trainName = args(0)
      outputFile = args(1)
    }

    // Percorso del dataset
    val trainPath = trainName

    println(s"Read train: $trainName")

    val conf = new SparkConf()
      .setAppName("CollaborativeFilteringDF")
      .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    // Inizializza una sessione Spark con master locale
    val spark = SparkSession.builder
      .config(conf)
      .appName("CollaborativeFilteringDF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Leggi il dataset dei voti dei film
    val ratings = spark.read
      .option("header", true)
      .csv(trainPath)

    // Preprocessing dei dati: converti i rating in tipo Double
    val ratingsDF = ratings
      .select(col("movieId"), col("userId"), col("rating").cast("Double"))

    println(s"Compute similarity for user $targetUser...")

    // Filtra solo i dati relativi all'utente target e calcola la similarità
    val targetRatings = ratingsDF.filter($"userId" === targetUser).alias("target")
    val similarity = ratingsDF
      .alias("others")
      .join(targetRatings, $"target.movieId" === $"others.movieId" && $"target.userId" =!= $"others.userId")
      .groupBy($"others.userId")
      .agg(
        sum($"target.rating" * $"others.rating").alias("dotProduct"),
        sqrt(sum(pow($"target.rating", 2))).alias("normTarget"),
        sqrt(sum(pow($"others.rating", 2))).alias("normOthers")
      )
      .withColumn("cosine_similarity", $"dotProduct" / ($"normTarget" * $"normOthers"))
      .select($"userId".alias("similarUserId"), $"cosine_similarity")
      .orderBy($"cosine_similarity".desc)

    println(s"Compute recommendations for user $targetUser...")

    // Calcola le raccomandazioni per l'utente target
    val recommendations = similarity
      .join(ratingsDF.alias("others"), $"similarUserId" === $"others.userId")
      .filter(!$"others.movieId".isin(targetRatings.select("movieId").collect().map(_.getString(0)): _*))
      .groupBy($"others.movieId")
      .agg(
        avg($"others.rating" * $"cosine_similarity").alias("predicted_rating")
      )
      .orderBy($"predicted_rating".desc)
      .limit(topN)
      .select(lit(targetUser).alias("userId"), $"others.movieId", $"predicted_rating")

    println(s"Saving top $topN recommendations for user $targetUser...")


    // Salva le raccomandazioni in un file CSV locale
    recommendations
      .coalesce(1)  // Unifica i file in uno solo
      .write
      .option("header", "true")
      .csv(outputFile)

    println(s"Recommendations saved successfully for user $targetUser.")

    // Chiudi la sessione Spark
    spark.close()
  }
}
