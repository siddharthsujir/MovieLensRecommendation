package com.regit.impl

import com.regit.caseclass.Ratings
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col,desc};
import org.apache.spark

object CollaborativeFilteringImplementation {


  def collaborativeFiltering(sparkSession: SparkSession): Unit ={

    // import customer rating data set

    var ratingSchema= new StructType(Array(
      new StructField("UserID",IntegerType,nullable = true),
      new StructField("MovieID",IntegerType,nullable = true),
      new StructField("Rating",IntegerType,nullable =true),
      new StructField("TimeStamp",StringType,nullable = true)
    )

    )
    import sparkSession.sqlContext.implicits._
    var ratings=sparkSession.read
      .option("headers","true")
      .option("sep","\t")
      .schema(ratingSchema)
      .format("csv")
      .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\ratings.dat")
        .as[Ratings]

    //ratings.show(100)

    var count=ratings.count()

    var distinctRatings= ratings.filter("UserID is not null").select("UserId","MovieID","Rating").distinct()

    var distinctCount=distinctRatings.count()

    //distinctRatings.show(100)
    print("Total Number of Ratings Available: "+count)

    print("\n Total Number of distinct ratings Available: "+distinctCount)


    var splitData=distinctRatings.randomSplit(Array(0.75,0.25))

    var trainingData=splitData(0);

    var testData=splitData(1);

    var trainingCount=trainingData.count();

    var testCount =testData.count();

    print("\n Training count "+trainingCount);

    print("\n Test Count "+ testCount)


    // Count per rating

    var countPerRating= distinctRatings.groupBy("Rating").count()
      .withColumn("CountOfRating",col("count"))
      .select("Rating","CountOfRating")

    countPerRating.show(100)

    var rated_Movie_perUser=distinctRatings.groupBy("UserID").count()
      .withColumn("UserRatingCount",col("count"))
      .select("UserID","UserRatingCount")


    //Number of Rated Movie Per user
    rated_Movie_perUser.orderBy(desc("UserRatingCount")).show(100)


    //Ratings per movie

    var ratings_per_Movie= distinctRatings.groupBy("MovieID").count()
      .withColumn("Rating_Count_perMovie",col("count"))
      .select("MovieID","Rating_Count_perMovie")

    ratings_per_Movie.orderBy(desc("Rating_Count_perMovie")).show(100)
  }





}
