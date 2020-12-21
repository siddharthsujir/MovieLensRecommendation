package com.regit.impl

import com.regit.caseclass.{MoviePair, Ratings}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col,sum,sqrt,count,when}

object MovieSimilarity {
  val sparkSession=SparkSession.builder()
    .master("local[*]")
    .appName("MovieSimilarity")
    .getOrCreate();

  import sparkSession.implicits._;

  def conputeSimilarity(): Unit={
    import org.apache.spark



    val ratingSchema=new StructType(Array(
      StructField("UserID",IntegerType,nullable = true),
      StructField("MovieID",IntegerType,nullable = true),
        StructField("rating",IntegerType,nullable = true),
        StructField("timeStamp",StringType,true)))



    var ratings=sparkSession.read
      .option("sep","\t")
      .option("headers","true")
      .schema(ratingSchema)
      .format("csv")
      .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\ratings.dat")
      .as[Ratings]

    ratings.show(100)

    var rating = ratings.as("ratings1").select("UserID","MovieID","rating")
      .join(ratings.as("ratings2"),$"ratings1.UserID"===$"ratings2.UserID" && $"ratings1.MovieID"<$"ratings2.MovieID")
      .select($"ratings1.MovieID".alias("movie1"),
      $"ratings2.MovieID".alias("movie2"),
      $"ratings1.rating".alias("rating1"),
      $"ratings2.rating".alias("rating2"))
        .as[MoviePair]

    rating.show(100)
    rating.persist()
    computeSimilarity(sparkSession,rating)
  }


  def computeSimilarity(sparkSession: SparkSession,data: Dataset[MoviePair] ): Unit={

    val pairScore=data
      .withColumn("xx",col("rating1")*col("rating1"))
      .withColumn("yy",col("rating2")*col("rating2"))
      .withColumn("xy",col("rating1")*col("rating2"))

    val calculateSimilarity=pairScore
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

    result.show(100)

  }

}
