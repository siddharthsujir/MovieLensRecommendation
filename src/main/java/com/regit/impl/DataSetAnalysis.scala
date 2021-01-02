package com.regit.impl

import com.regit.caseclass.{Book, Person, WeatherData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, split};

object DataSetAnalysis {


  def main(args: Array[String])= {


    val sparkSession= SparkSession.builder()
      .appName("friendsAnalysis")
      .master("local[*]")
      .getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR")
    //friendsByAge(sparkSession)
    //wordCountWithDS(sparkSession)

    //minimumTemperatureByWeatherStation(sparkSession)


   // MovieAnalysis.findMostPopularMovie(sparkSession)

   // SuperHeroAnalysis.mostPopularSuperHero(sparkSession);

    //SuperHeroAnalysis.mostObscureSuperHero(sparkSession)

   // MovieSimilarity.conputeSimilarity();

    CollaborativeFilteringImplementation.collaborativeFiltering(sparkSession);

    //session could be running beyond the lifetime if
    //we dont explicitly stop it
    sparkSession.stop();
  }

  def friendsLoad(sparkSession: SparkSession):Unit={

    import sparkSession.sqlContext.implicits._;
    var person=sparkSession.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\fakefriends.csv")
      .as[Person]

    person.select("id","name","age").where("age<=18").show(100);
    //session could be running beyond the lifetime if
    //we dont explicitly stop it

  }

  def friendsByAge(sparkSession: SparkSession)={

    import sparkSession.sqlContext.implicits._;
    var person=sparkSession.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\fakefriends.csv")
      .as[Person]

    var friendsByAge=person
      .groupBy("age")
      .avg("friends")
      //.as("sum")
      .withColumn("ageSum",col("avg(friends)"))
      .select("age","ageSum")
      .orderBy("age")
      .show(100)

  }


  def wordCountWithDS(sparkSession: SparkSession):Unit= {

    import sparkSession.sqlContext.implicits._
    val book=sparkSession
      .read
      .text("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\book.txt")
      .as[Book]

    book.show(100)

    val words=book.select(explode(split($"value","\\W+")).alias("words"))
      .filter($"words"=!="")

    words.show(1000);
    var wordCount=words.groupBy("words")
        .count()
        .alias("wordCount")

    wordCount.show(1000)

  }

  def minimumTemperatureByWeatherStation(sparkSession: SparkSession): Unit = {

    import sparkSession.sqlContext.implicits._;
    var weatherData= sparkSession.read
        .option("header","true")
      .option("inferSchema","true")
      .csv("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\1800.csv")
      .as[WeatherData]

    weatherData.show(1000)

    var weatherFiltered =weatherData.filter($"MaxMinType"==="TMIN")
      .groupBy("stationID")
      .min("temperature")
      .withColumn("MiniumunTemperature",col("min(temperature)"))

    weatherFiltered.show(1000)
  }

}
