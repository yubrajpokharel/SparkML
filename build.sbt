name := "SparkML"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies++={
  val sparkVersion  = "2.2.0"
  Seq(
    "org.apache.spark"  %%  "spark-core"      % sparkVersion,
    "org.apache.spark"  %%  "spark-sql"       % "2.2.0"
  )
}