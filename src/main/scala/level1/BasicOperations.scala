package level1

import org.apache.log4j.{Level, Logger}
import utils.SparkCommonUtil

/**
  * Created by yubrajpokharel on 2/6/18.
  */
object BasicOperations extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sc = SparkCommonUtil.spContext
  val data = sc.parallelize(Array(2,3,4,5,3,4))
  data.cache()
  data.count()

  println("total count : " + data.count())

  /**
    * getting the data from the external file
    */
  val autoData = sc.textFile(SparkCommonUtil.dataDir + "auto-data.csv")
  autoData.cache()
  val partialData = autoData.take(10)
  println("Total auto record :: " + autoData.count())

  for(x <- partialData)
    println(x)


}