package com.regit.impl

import com.regit.caseclass.{SuperHero, SuperHeroNames}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{split,col,size,desc,sum,min};

object SuperHeroAnalysis {

  def mostPopularSuperHero(sparkSession: SparkSession): Unit ={

    val superHeroNamesSchema =new StructType()
      .add("id",IntegerType,nullable = true)
      .add("name",StringType,nullable = true)

    import sparkSession.implicits._

    val names= sparkSession.read.schema(superHeroNamesSchema)
      .option("sep"," ")
      .csv("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-names.txt")
      .as[SuperHeroNames]

    val lines=sparkSession.read
      .text("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-graph.txt")
        .as[SuperHero]


    val connection = lines
        .withColumn("id",split(col("value")," ")(0))
        .withColumn("connection",size(split(col("value")," "))-1)
        .groupBy("id")
        .sum("connection")
        .join(names,"id")
        .orderBy(desc("sum(connection)"))
        .select("name","sum(connection)")

   // names.show

    connection.show(1000)
  }


  def mostObscureSuperHero(sparkSession: SparkSession): Unit = {

    val superHeroSchema= new StructType()
      .add("id",IntegerType,nullable = true)
      .add("name",StringType,nullable = true)

    import sparkSession.implicits._
    val superHeroName=sparkSession.read.schema(superHeroSchema)
      .option("sep"," ")
      .csv("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-names.txt")
      .as[SuperHeroNames]


    val lines=sparkSession.read
      .text("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\Marvel-graph.txt")
      .as[SuperHero]

    val connections=lines.
      withColumn("id",split(col("value")," ")(0))
      .withColumn("connections",size(split(col("value")," "))-1)
      .groupBy("id")
      .agg(sum("connections").alias("totalConnections"))
      //.filter($"totalConnections"===1)

    val minimumConnection=connections.select(min("totalConnections")).first().getLong(0)

    print("print",minimumConnection)


    val obscureSuperHeroName=connections
      .filter($"totalConnections"===minimumConnection)
      .join(superHeroName,"id")
      .select("name")


    obscureSuperHeroName.show(100)

  }
}
