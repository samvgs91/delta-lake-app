package com.dcc.delta

import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.types.StructType
import java.io.File
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scala.io.Source



object DeltaLakeNewSchema extends App{

  private var mappingDataTypes = Map[String,DataType]()

private val json =
"""
        { "order": 1,"columnName": "id","dataType": "StringType" }
""".stripMargin



  val source: String = Source.fromFile("src/resources/mapping.json").getLines.mkString
  val mapConf: JsValue = Json.parse(source)

  case class Column(order:Int, name:String, dataType:String)
  case class ColumnMapping(name:String, mappings: List[Column]) 

  implicit val readColumn: Reads[Column] = (
      (JsPath \ "order").read[Int] and
      (JsPath \ "columnName").read[String] and
      (JsPath \ "dataType").read[String]
  )(Column.apply _)

  implicit val readMapping: Reads[ColumnMapping] = (
      (JsPath \ "name").read[String] and
      (JsPath \ "mapping").read[List[Column]]
  )(ColumnMapping.apply _)

  val parsedJsValue = Json.parse(json)
  val parsed = Json.fromJson[Column](parsedJsValue)


  
  def parseSchema(): StructType = { 
    null
  }


  val Spark = SparkSession.builder()
  .appName("DeltaApp")
  .master("local")
  .config("spark.executor.instances","2")
  .getOrCreate()

  //read incoming data 
  //val newDataDf = Spark.read.option("header",true).csv("C:\\Users\\SAMUEL JURADO\\Documents\\OTR\\Code\\Spark\\delta-lake-app\\delta-lake-app\\products3.csv")

  //replace the entire dataset

  //newDataDf.write.format("delta").mode("append").option("mergeSchema","true").save("/delta-table/product")

  //val defaultSchema = StructTy



  val df = Spark.read.format("delta").load("/delta-table/product").where("id >= 10000")


  val dfSchema = df.schema
  df.show()
  println("DataFrame Schema: "+dfSchema.toString())

  println("Parsed :"+parsed.toString())
  println("Mapping Json Load: "+mapConf)

  
}
