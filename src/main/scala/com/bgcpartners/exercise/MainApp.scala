package com.bgcpartners.exercise

import org.apache.spark.sql.SparkSession

/**
  * Created by adnan_saqib on 09/11/2019.
  */
object MainApp extends App {

  val appName = "imdb-movie-analyzer"
  val master = "local[*]" //  In order to run application in yarn we need to set master to yarn and set yarn queue
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions","2") //optimised for standalone mode
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName(appName)
    .master(master)
    .getOrCreate()

  val imdbDownloadedDataPath: String = args(0)
  val resultsOutputPath: String = args(1)

  val input = Map.newBuilder[String, String]
  input.+=("NAME_BASICS_TSV" -> s"${imdbDownloadedDataPath}/name.basics.tsv.gz")
  input.+=("TITLE_BASICS_TSV" -> s"${imdbDownloadedDataPath}/title.basics.tsv.gz")
  input.+=("TITLE_PRINCIPALS_TSV" -> s"${imdbDownloadedDataPath}/title.principals.tsv.gz")
  input.+=("TITLE_RATINGS_TSV" -> s"${imdbDownloadedDataPath}/title.ratings.tsv.gz")

  val movieDataAnalyzer = new MovieDataAnalyzer(spark)
  val movieDataExtracted = movieDataAnalyzer.extract(input.result())
  val movieDataTransformed = movieDataAnalyzer.transform(movieDataExtracted)
  movieDataAnalyzer.load(movieDataTransformed, resultsOutputPath)
}
