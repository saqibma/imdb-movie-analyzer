package com.bgcpartners.exercise

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class TitleRatings (tconst: String, averageRating: String, numVotes: String)

class MovieDataAnalyzer(val spark: SparkSession) extends Serializable {

  def extract(input: Map[String, String]): Map[String, DataFrame] = {
    //Reading data from name.basics.tsv.gz
    val nameBasicsDf = createDF(input.get("NAME_BASICS_TSV").get )

    //Reading data from title.basics.tsv.gz
    val titleBasicsDf = createDF(input.get("TITLE_BASICS_TSV").get)

    //Reading data from title.principals.tsv.gz
    val titlePrincipalsDf = createDF(input.get("TITLE_PRINCIPALS_TSV").get)

    //Reading data from title.ratings.tsv
    val titleRatingsDf = createDF(input.get("TITLE_RATINGS_TSV").get)

    val extractedDF = Map.newBuilder[String, DataFrame]
    extractedDF.+=("NAME_BASICS_DF" -> nameBasicsDf)
    extractedDF.+=("TITLE_BASICS_DF" -> titleBasicsDf)
    extractedDF.+=("TITLE_PRINCIPALS_DF" -> titlePrincipalsDf)
    extractedDF.+=("TITLE_RATINGS_DF" -> titleRatingsDf)
    extractedDF.result()
  }

  def createDF(filePath: String) : DataFrame =
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", "\t")
      .csv(filePath)

  def transform(movieDataExtracted: Map[String, DataFrame]): DataFrame = {
    import spark.implicits._
    val titleRatingsDS = movieDataExtracted.get("TITLE_RATINGS_DF").get.as[TitleRatings]
    titleRatingsDS.persist()

    val top20MoviesDf = retrieveTopTwentyMovies(titleRatingsDS)

    val titleBasicsDf = movieDataExtracted.get("TITLE_BASICS_DF").get
    // Retrieving top 20 movies with list of titles
    val top20MoviesWithTitlesDf = titleBasicsDf
      .join(broadcast(top20MoviesDf), "tconst")
      . select(col("tconst"), concat_ws(",", col("primaryTitle"), col("originalTitle")).alias("list of titles"))

    val titlePrincipalsDf = movieDataExtracted.get("TITLE_PRINCIPALS_DF").get
    val top20MoviesWithPrincipalsDf = titlePrincipalsDf
      .join(broadcast(top20MoviesDf), "tconst")
      .select(col("tconst"), col("nconst"))

    val nameBasicsDf = movieDataExtracted.get("NAME_BASICS_DF").get
    val top20MoviesWithNameBasicsDf = nameBasicsDf
      .join(broadcast(top20MoviesWithPrincipalsDf), "nconst")
      .withColumn("mostCreditedForTitles", split(col("knownForTitles"), ","))
      .select(col("tconst"),
        col("nconst"),
        col("primaryName"),
        col("knownForTitles") ,
        explode(col("mostCreditedForTitles")).alias("most credited for titles"))

    val top20MoviesWithMostCreditedPersonsDf = top20MoviesWithNameBasicsDf
      .join(broadcast(top20MoviesDf), top20MoviesWithNameBasicsDf.col("most credited for titles") === top20MoviesDf.col("tconst"))
      .groupBy(top20MoviesDf.col("tconst"))
      .agg(concat_ws(",", collect_set(col("primaryName"))).alias("most credited persons"))

    top20MoviesWithTitlesDf.join(top20MoviesWithMostCreditedPersonsDf, "tconst")
  }

  def retrieveTopTwentyMovies(titleRatingsDS: Dataset[TitleRatings]) = {
    // Calculating average number of votes
    val averageNumberOfVotesDf = titleRatingsDS
      .agg(avg("numVotes").alias("averageNumberOfVotes"))
    val averageNumberOfVotes = averageNumberOfVotesDf.first().getDouble(0)

    // Adding a new column called ranking calculated by the equation (numVotes/averageNumberOfVotes) * averageRating
    val titleRatingsWithRankingDf = titleRatingsDS
      .withColumn("ranking", expr(s"(numVotes/${averageNumberOfVotes}) * averageRating"))

    // Retrieving the top 20 movies with a minimum of 50 votes with the ranking determined above
    val top20MoviesDf = titleRatingsWithRankingDf
      .where("numVotes >= 50")
      .orderBy(col("ranking").desc)
      .select("tconst")
      .limit(20)
    top20MoviesDf.persist()
    top20MoviesDf
  }

  def load(movieDataTransformed: DataFrame, resultsOutputPath: String): Unit =
    movieDataTransformed
      .coalesce(1)
      .write
      .format("csv")
      .option("sep", "\t")
      .option("mode","OVERWRITE")
      .option("header","true")
      .save(resultsOutputPath)
}
