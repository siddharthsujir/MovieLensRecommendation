import org.apache.spark.SparkContext

object RatingsAnalysis extends App {


  //analyzeRatingWithRDD();

  //weatherStationMinimumTemperature();

  avgSpendByCustomer()

  // read the ratings file and count how many times a rating appears uniquely
  def analyzeRatingWithRDD(): Unit ={

    val sc= new SparkContext("local[*]","RatingsAnalysis");

    var rdd1=sc.textFile("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\Data\\ml-1m\\ratings.dat")

    val rdd2= rdd1.map(s=>s.split("::")(2)).map(s=>(s,1)).reduceByKey((x,y)=>x+y)

    var arr=rdd2.sortBy(_._1).collect()
    arr.foreach(print)

  }

  def weatherStationMinimumTemperature() ={

    val sc=new SparkContext("local[*]","WeatherApp")

    val weatherData=sc.textFile("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\1800.csv")

    val filteredData=weatherData.map(s=>s.split(",")).map(s=>(s(0),s(2),s(3).toFloat)).filter(x=>x._2=="TMIN")

    var minimumByStation=filteredData.map(s=>(s._1,s._3.toFloat)).reduceByKey((x,y)=> if (x<y) x else y);
    minimumByStation.collect().foreach(println)

  }

  def avgSpendByCustomer()={

    val sc= new SparkContext("local[*]","CustomerSpendAverage")

    val customerData=sc.textFile("C:\\Users\\siddhu\\Documents\\spark-scala-training-11202020\\SparkScalaCourse\\SparkScalaCourse\\data\\customer-orders.csv")

    val customerTuple=customerData.map(s=>s.split(",")).map(s=>(s(0),s(2).toFloat)).reduceByKey((x,y)=>x+y)

    customerTuple.collect().foreach(println)

  }
}
