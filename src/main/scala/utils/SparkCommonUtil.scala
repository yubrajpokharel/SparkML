package utils

/**
  * Created by yubrajpokharel on 2/6/18.
  */
object SparkCommonUtil {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  var dataDir = "src/data-files/"
  var appName = "sparkML"
  var sparkMasterUrl = "local[*]"
  var tempDir = "src/temp"

  var spSession:SparkSession  = null
  var spContext:SparkContext = null

  {

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMasterUrl)
      .set("spark.executor.memory","2g")
      .set("spark.sql.shuffle.partitions","2")

    //Get or create a spark context. Creates a new instance if not exists
    spContext = SparkContext.getOrCreate(conf)

    //Create a spark SQL session
    spSession = SparkSession
      .builder()
      .appName(appName)
      .master(sparkMasterUrl)
      .config("spark.sql.warehouse.dir", tempDir)
      .getOrCreate()

  }


}
