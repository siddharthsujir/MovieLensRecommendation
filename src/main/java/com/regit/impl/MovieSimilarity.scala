package com.regit.impl

import com.regit.caseclass.{Movie, MoviePair, MoviePairSimilarity, Ratings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, count, sqrt, sum, when}

object MovieSimilarity {
  val sparkSession=SparkSession.builder()
    .master("local[*]")
    .appName("MovieSimilarity")
    .getOrCreate();

  import sparkSession.implicits._;

  def conputeSimilarity(): Unit={



    val ratingSchema=new StructType(Array(
      StructField("UserID",IntegerType,nullable = true),
      StructField("MovieID",IntegerType,nullable = true),
        StructField("rating",IntegerType,nullable = true),
        StructField("timeStamp",StringType,true)))


    var movieSchema = new StructType(Array(
      StructField("MovieID",IntegerType,nullable = true),
      StructField("Title",StringType,nullable = true),
      StructField("Genres",StringType,nullable = true)
    ))

    var ratings=sparkSession.read
      .option("sep","\t")
      .option("headers","true")
      .schema(ratingSchema)
      .format("csv")
      .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\ratings.dat")
      .as[Ratings]

    var movieName=sparkSession.read
        .option("sep","\t")
        .option("headers","true")
        .schema(movieSchema)
        .format("csv")
        .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\movies.dat")
        .as[Movie]

    val movies=movieName.filter("MovieID is not null")
//
    ratings.show(100)
    ratings.persist();
    var rating = ratings.as("ratings1").select("UserID","MovieID","rating")
      .join(ratings.as("ratings2"),$"ratings1.UserID"===$"ratings2.UserID" && $"ratings1.MovieID"<$"ratings2.MovieID")
      .select($"ratings1.MovieID".alias("movie1"),
      $"ratings2.MovieID".alias("movie2"),
      $"ratings1.rating".alias("rating1"),
      $"ratings2.rating".alias("rating2"))
        .as[MoviePair]

    rating.show(100)
    rating.persist()
    val moviePairSimilarity=computeSimilarity(sparkSession,rating)

    var scoreThreshold=0.97
    var coOccurenceThreshold=50.0

    val movieId:Int=1

    var filteredResults=moviePairSimilarity.filter(
      (col("movie1")===movieId|| col("movie2")===movieId) &&
      col("score")>scoreThreshold && col("numPairs")>coOccurenceThreshold
    )

    var results=filteredResults.sort(col("score").desc).take(10)

    for(result <-results){
      var similarMovieID = result.movie1

      if(similarMovieID==movieId)
        {
          similarMovieID=result.movie2
        }

      print(getMovieName(sparkSession,movies,similarMovieID)+" score "+result.score+" numPairs "+result.numPairs+" \n")

    }

  }

  def getMovieName(sparkSession: SparkSession,movieNames:Dataset[Movie],movieId:Int):String = {



    val map2=movieNames.map(r=>(r.MovieID,r.Title))
      .collect().toMap


    map2.get(movieId).toString();
  }


  def computeSimilarity(sparkSession: SparkSession,data: Dataset[MoviePair] ): Dataset[MoviePairSimilarity]={

    val pairScore=data
      .withColumn("xx",col("rating1")*col("rating1"))
      .withColumn("yy",col("rating2")*col("rating2"))
      .withColumn("xy",col("rating1")*col("rating2"))

    val calculateSimilarity=pairScore.filter("rating1>3 and rating2>3")
      .groupBy("movie1","movie2")
      .agg(
        sum(col("xy")).alias("numerator"),(sqrt(sum(col("xx")))*sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")

      )

    val result=calculateSimilarity
      .withColumn("score",
        when(col("denominator")=!=0,col("numerator")/col("denominator"))
          .otherwise(null)
      ).select("movie1","movie2","score","numPairs")
        .as[MoviePairSimilarity]

    result.show(100)
result
  }

}
