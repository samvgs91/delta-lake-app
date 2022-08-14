package com.dcc.delta

import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit

import java.io.File

object DeltaLakeApp extends App {

  val Spark = SparkSession.builder()
  .appName("DeltaApp")
  .master("local")
  .config("spark.executor.instances","2")
  .getOrCreate()

  //Reading a file
  //val df = Spark.read.option("header",true).csv("C:\\Users\\Aravinth\\Datalake\\products.csv")


  val df = Spark.read.option("header",true).csv("C:\\Users\\SAMUEL JURADO\\Documents\\OTR\\Code\\Spark\\delta-lake-app\\delta-lake-app\\products2.csv")

  //df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save("/delta-table/product")

  val rdf = Spark.read.format("delta").load("/delta-table/product")

//  rdf.show()

  val newdf = rdf.withColumn("Country",lit("USA"))

  newdf.write.format("delta").mode("overwrite").option("mergeSchema","true").save("/delta-table/product")

  val df2 = Spark.read.format("delta").load("/delta-table/product")
  df2.show()

  val df_before = Spark.read.format("delta").option("versionAsOf",0).load("/delta-table/product")
  df_before.show()
  
  //df2.count()

}