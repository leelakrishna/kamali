package com.kamali.excercises;

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions


object SPARKsql{
  
  def main(args:Array[String]){
  
  val spark=SparkSession.builder().master("local[*]").appName("SPARKsql").getOrCreate()
 
   import spark.implicits
   spark.sparkContext.setLogLevel("WARN")
   val tschema=StructType(Array(
       StructField("Fid",DoubleType),
       StructField("name",StringType),
       StructField("fcount",StringType),
       StructField("f2count",StringType)
       ))
  
   
  
       
val dataFrame2=spark.read.schema(tschema).csv("/home/kiran/SparkScala/fakefriends.csv")
dataFrame2.show()
dataFrame2.schema.printTreeString()
  
   val f2max=dataFrame2.filter("f2count>300")
   val fidmax=dataFrame2.filter("Fid>10")
   
   val joined= f2max.join(fidmax,f2max("Fid")===fidmax("Fid"))
   f2max.show()
  joined.show()
 val newjoined=fidmax.groupBy("name").count()
 println("new")
 newjoined.show()

   spark.stop()
  }
}

