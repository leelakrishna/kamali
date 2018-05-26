package com.kamali.exe
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
object mEngine {

   
  def main(args:Array[String]) 
  {
  
   val schema=StructType(Array(
       StructField("userID",IntegerType),
       StructField("ArtistID",IntegerType),
       StructField("Rating",IntegerType)))
   
  //   val sc =new SparkContext("local[*]","mEngine")
  val uData= spark.read.textFile("/home/kiran/music_REC/user_artist_data_.txt")
  
  val spark=SparkSession.builder().master("local[*]").appName("mEngine").getOrCreate()
  import spark.implicits
  spark.sparkContext.setLogLevel("WARN")
   
   uData.take(5).foreach(println)

   //this peice of code shows arguemnts error in IDE
  val userArtistDF = uData.map { line =>
          val Array(user, artist, _*) = line.split(' ') 
          (user.toInt, artist.toInt)
         }.toDF("user", "artist")
   spark.stop()
  }  
  
}