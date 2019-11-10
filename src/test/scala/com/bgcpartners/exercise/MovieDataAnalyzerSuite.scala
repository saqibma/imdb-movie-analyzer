package com.bgcpartners.exercise

/**
 * Created by adnan_saqib on 10/11/2019.
 */
class MovieDataAnalyzerSuite extends BaseSuite {
  private var movieDataAnalyzer: MovieDataAnalyzer = _

  override def beforeAll() {
    super.beforeAll()
    movieDataAnalyzer = new MovieDataAnalyzer(spark)
  }

  test("Retrieve top 20 movies, list the persons who are most often credited and list the titles of the 20 movies") {
    Given("IMDb data files downloaded")
    val imdbDownloadedDataSetPath = "D:/data" // Please provide the right location for the downloaded imdb dataset
    val input = Map.newBuilder[String, String]
    input.+=("NAME_BASICS_TSV" -> s"${imdbDownloadedDataSetPath}/name.basics.tsv.gz")
    input.+=("TITLE_BASICS_TSV" -> s"${imdbDownloadedDataSetPath}/title.basics.tsv.gz")
    input.+=("TITLE_PRINCIPALS_TSV" -> s"${imdbDownloadedDataSetPath}/title.principals.tsv.gz")
    input.+=("TITLE_RATINGS_TSV" -> s"${imdbDownloadedDataSetPath}/title.ratings.tsv.gz")

    When("Call MovieDataAnalyzer extract and transform methods")
    val movieDataExtracted = movieDataAnalyzer.extract(input.result())
    val actualResult = movieDataAnalyzer.transform(movieDataExtracted)

    Then("Compare actual and expected data frames")
    val expectedResult = createDF("result.tsv")
    assertDataFrameEquals(actualResult, expectedResult)
  }

  test("Retrieve the top 20 movies with a minimum of 50 votes with the ranking determined by " +
    "(numVotes/averageNumberOfVotes) * averageRating") {
    Given("Downloaded Title Ratings TSV file")
    val titleRatingsDS = createDF("title.ratings.tsv.gz").as(org.apache.spark.sql.Encoders.product[TitleRatings])

    When("Call MovieDataAnalyzer retrieveTopTwentyMovies method")
    val top20MoviesDf = movieDataAnalyzer.retrieveTopTwentyMovies(titleRatingsDS)

    Then("Compare actual and expected data frames")
    val expectedResult = createDF("top20movies.tsv")
    assertDataFrameEquals(top20MoviesDf, expectedResult)
  }
}
