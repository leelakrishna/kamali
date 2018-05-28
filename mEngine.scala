package com.kamali.exe
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{max,min}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Dataset
import org. apache. spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.recommendation._
import scala.util.Random
object mEngine {

   


  def main(args:Array[String]) 
  {
  
    //schema for UserData DataFrame
   val schema=StructType(Array(
       StructField("userID",IntegerType),
       StructField("ArtistID",IntegerType),
       StructField("Rating",IntegerType)
         )
       )
      
     //schema for ArtistData DataFrame  
val schema2=StructType(Array(
       StructField("ArtistID",IntegerType),
       StructField("ArtistName",StringType))
       )

val schema3=StructType(Array(
       StructField("ArtistID",IntegerType),
       StructField("AliasID",IntegerType))
       )
       
  
    
   val spark=SparkSession.builder().master("local[*]").appName("SPARKsql").getOrCreate()
   import spark.implicits
   spark.sparkContext.setLogLevel("WARN")
   //Reading user File
    val userDataset=spark.read.textFile("/home/kiran/music_REC/userartistdata.txt")
    
    //cleansing User Data
    val Processedrdd=userDataset.rdd.map{ line=>
      val k=line.split(" ")
      val user=k.apply(0).toInt
      val artist=k.apply(1).toInt
      val rating=k.apply(2).toInt
      Row(user,artist,rating)
   }

  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   
  //created User DataFrame
  val UserData=spark.createDataFrame(Processedrdd,schema)
    //prnitng for testing purpose
   UserData.limit(5).show()
 
 //finding min and max length of USERID and ARTISTID from userData to iterate
 val agg=UserData.agg(min("userID"),max("userID"),min("ArtistID"),max("ArtistID")) 
 
 agg.show() 
 
 //Reading Artitst data file
 val Artists=spark.read.textFile("/home/kiran/music_REC/artistdata.txt")
 
 //cleansing Artists Data
  val b=Artists.rdd.map{line=>

  val l=line.length()
  
  val id= line.slice(0, 6)
  val name=line.slice(8,l)
  Row(id.toInt,name.toString)
  
  }
  
  //Creating Artists Data
  val ArtistData=spark.createDataFrame(b,schema2)
  ArtistData.show()
  
 val Alias=spark.read.textFile("/home/kiran/music_REC/artistalias.txt")
    
    //cleansing User Data
    val ProcessedAlias=Alias.rdd.map{ line=>
      val k=line.split("\t").toArray
      val ArtistID=k.apply(0).toInt
      val AliasID=k.apply(1).toInt
      Row(ArtistID,AliasID)
  }
  val AliasMap=ProcessedAlias.collect()
  val AliasData=spark.createDataFrame(ProcessedAlias,schema3)
   AliasData.show() 

   def buildCounts(UserData:DataFrame,AliasData:DataFrame):DataFrame={
      
    val k=UserData.join(AliasData,UserData("ArtistID")===AliasData("ArtistID"),"leftanti")
      
  (k)
  }
  
  val trainingData = buildCounts(UserData,AliasData)
  trainingData.limit(10).show()
  trainingData.cache()
  val model = new ALS().
     setSeed(Random.nextLong()).
     setImplicitPrefs(true).
     setRank(10).
     setRegParam(0.01).
     setAlpha(1.0).
     setMaxIter(5).
     setUserCol("userID").
     setItemCol("ArtistID").
     setRatingCol("Rating").
     setPredictionCol("prediction").
     fit(trainingData)
  
  
    spark.stop()
       
   
  
  }
  
}