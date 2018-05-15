package sparkSql

import org.apache.log4j.Logger
import org.apache.log4j.Level
import utils.SparkCommonUtil


/**
  * Created by yubrajpokharel on 5/13/18.
  */
object SparkSql extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spartSession = SparkCommonUtil.spSession
  val spContext = SparkCommonUtil.spContext
  val datadir = SparkCommonUtil.dataDir

//  create dataframe
  val empDf = spartSession.read.json(datadir+"customerData.json")
  empDf.show()
  empDf.printSchema()

//  dataFrame Queries

  println("showing the name of employees")
  empDf.select("name").show()

  println("filter employee's by the gender male")
  empDf.filter(empDf("gender") === "male").show()
  empDf.filter(empDf("salary > 45000")).show()

  println("group by employee by the gender")
  empDf.groupBy(empDf("gender")).count().show()

  println("some basic aggerate examples")
  val emptemp = empDf.groupBy("deptid")
//       emptemp.agg(avg(empDf("salary")), max(empDf("age"))).show()




}
