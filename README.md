Spark Based IMDb Movie Data Analytics App

Please go through the following points before running the application.
1) Following datasets were downloaded from https://datasets.imdbws.com/ and kept under resources folder 
    i) name.basics.tsv.gz
    ii) title.basics.tsv.gz
    iii) title.principals.tsv.gz
    iv) title.ratings.tsv.gz
2) Please run the main program MainApp.scala in order to perform IMDb Movie Data analytics.
3) Please provide full path to the output folder as an argument to the main program.
   For example C:\dev\result
4) Please make sure to delete output files and folders produced before running the main program second time.
5) MovieDataAnalyzer performs extraction, transformation and load activities.
   Please refer MovieDataAnalyzer.scala file.
6) MovieDataAnalyzer extract method parses and extracts movie datasets downloaded from https://datasets.imdbws.com/.
7) MovieDataAnalyzer transform method performs all the transformations asked in the exercise and find:
   a) Top 20 movies with a minimum of 50 votes with the ranking determined by 
      (numVotes/averageNumberOfVotes) * averageRating
   b) Then it list the persons who are most often credited and list the different titles of the top 20 movies.   
8) MovieDataAnalyzer load method saves transformed dataframes as csv files(with header) under the output
   folder specified as an argument to the main program.
9) Integration testing was done in a BDD style for each transformations asked in the exercise.
    Please refer MovieDataAnalyzerSuite.scala class.
10) Please run MovieDataAnalyzerSuite.scala class in order to test all the transformations.
Please provide the right location for the downloaded imdb dataset.
11) Transformed dataframes are being validated against the pre-calculated results present in csv files under
    src\test\resources directory as a part of the integration test
12) A generic spark based test framework was designed and developed to test all kinds of RDDs and data frames.
    Please refer BaseSuite.scala class for the detail implementation.
13) pom.xml file was updated to create a jar with dependencies called uber jar.

