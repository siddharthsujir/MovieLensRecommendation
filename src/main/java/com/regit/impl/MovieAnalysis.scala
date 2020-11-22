package com.regit.impl

import com.regit.caseclass.{Movie, Ratings}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc,col,udf}


import scala.io.Source

object MovieAnalysis {

  // Popularity in terms of most viewed
  def findMostPopularMovie(sparkSession: SparkSession):Unit={

    import sparkSession.sqlContext.implicits._
    var movies=sparkSession.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep","\t")
      .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\movies.dat")
      .as[Movie]

    movies.show(100);

    var movieDict=sparkSession.sparkContext.broadcast(loadMoviesToMap(sparkSession));

    var lookupName:Int =>String = (movieID:Int)=>{
      print(movieID)
      movieDict.value(movieID);
    }

    val lookupNamesUDF=udf(lookupName);

    var ratings=sparkSession.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep","\t")
      .load("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\ratings.dat")
      .as[Ratings]

    ratings.show(100)


    var popularMovie=ratings.groupBy("MovieID").count()
      .join(movies,"MovieID")
      //.withColumn("movieName",lookupNamesUDF(col("MovieID")))
      .orderBy(desc("count"))
      .select("Title","count")


    popularMovie.show(1)
  }

  def loadMoviesToMap(sparkSession: SparkSession):Map[Int,String]={

    var lines = Source.fromFile("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\movies.dat")
    var movieName: Map[Int,String]=Map()
    for(line <- lines.getLines()){
      var field1=line.split("\t")
      if(field1.length>1)
        {
          movieName += (field1(0).toInt -> field1(1))
        }
    }
    lines.close();
    movieName
  }

}
