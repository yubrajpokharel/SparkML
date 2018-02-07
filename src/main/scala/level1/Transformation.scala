package level1

import utils.SparkCommonUtil

/**
  * Created by yubrajpokharel on 2/6/18.
  */
object Transformation extends App{

  val sc = SparkCommonUtil.spContext

  val data = sc.textFile(SparkCommonUtil.dataDir + "iris.csv")
  data.cache()

  println("total records :: " + data.count())

  val tempData = data.map(x => x.replace(",", "\t")).take(10)
  for(x <- tempData) println(x)

  val sepalLengthEqualsto5 = data.filter(x => x.contains("5")).take(10)
  for(x <- sepalLengthEqualsto5) println(x)


  /**
    * calculate the average miles of each car
  */
  val carData = sc.textFile(SparkCommonUtil.dataDir + "auto-data.csv")
  carData.cache()

  def isDigit(x: String) = x.matches("^\\d*$")

  def getMPG(autoStr: String): String = {
    if(isDigit(autoStr)) return autoStr
    else {
      val attList = autoStr.split(",")
      if(isDigit(attList(9))) return attList(9)
      else return "0"
    }
  }

  def totalMPG = carData.reduce((x, y) => (getMPG(x).toInt + getMPG(y).toInt).toString)
  def totalCars = carData.count()

  println("Average Milage of all cars :: " + (totalMPG.toInt / totalCars.toInt))

  def getCarBrandName(x: String): String = {
    val singleData = x.split(",")
    return singleData(0)
  }

  def getBrands: Array[String] = {
    val header = carData.first()

    val allBrands = carData
                .filter(x => x!=header)
                .map(x => getCarBrandName(x))
                .distinct()
                .collect()

    return allBrands
  }

  var singleBrandData  = Array[String]()


  for(b <- getBrands) println(s"$b")

  val header = carData.first()
  //creating key value RDD
  val autoHPData = carData
    .filter(x => x != header)
    .map(x => (x.split(",")(0), x.split(",")(7)))

  println("AutoData ----------------------------------")
  autoHPData.keys.distinct().collect()
  for(x <- autoHPData) println(x)


  val brandValue = autoHPData.mapValues(x => (x.toInt, 1))
  brandValue.collect()
  println("brandvalue ----------------------------------")
  for(x <- brandValue) println(x)


  val totalMPG2 = brandValue.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  totalMPG2.collect()

  val result = totalMPG2.mapValues(x => x._1 / x._2).collect()
  println("result ----------------------------------")
  for(x <- result) println(x)

  Thread.sleep(10000)

}
