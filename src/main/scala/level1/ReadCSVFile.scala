package level1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yubrajpokharel on 1/30/18.
  */
object ReadCSVFile extends App{

  val appName = "csvReader"
  val url = "local"

  val config = new SparkConf()
    .setAppName(appName)
    .setMaster(url)
    .set("spark.executor.memory", "2g");

  val sparkContext = SparkContext.getOrCreate(config)

  val tweetRDD = sparkContext.textFile("src/data-files/movietweets.csv")

  for(t <- tweetRDD.take(10)) println(t)

  println("total lines in the $tweetRDD file ==> " + tweetRDD.count())

}
