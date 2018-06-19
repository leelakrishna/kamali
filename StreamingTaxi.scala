package com.kamali.exe
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
//import org.apache.hadoop.fs.s3a.S3AFileSystem 
//import org.apache.hadoop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions._
object StreamingTaxi{
 

  def main(args: Array[String]) {
   
  
   //sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJY3KKR5VZK4VKCPQ")
   //sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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") // can contain "/"
 
  
    //val data=sc.textFile("s3n://s3.amazonaws.com/nyc-tlc/trip+data/*.csv")
     
  val spark=SparkSession.builder().master("local").appName("Text2SQL").getOrCreate()
  import spark.implicits
  spark.sparkContext.setLogLevel("WARN")
 val OurSchema=StructType(Array(
 StructField("vendorID",IntegerType,true),
 StructField("pickupDatetime",TimestampType,true),
 StructField("dropDateTime",TimestampType,true),
 StructField("NoOfPass",IntegerType,true),
 StructField("tripDistance",DoubleType,true),
 StructField("RatecodeID",IntegerType,true),
 StructField("StoreFwdFlag",StringType,true),
 StructField("PULocationID",IntegerType,true),
 StructField("DOLocationID",IntegerType,true),
 StructField("PaymentType",IntegerType,true),
 StructField("fare",DoubleType,true),
 StructField("extra",DoubleType,true),
 StructField("mtaTax",DoubleType,true),
 StructField("tip",DoubleType,true),
 StructField("tolls",DoubleType,true),
 StructField("improveCharge",DoubleType,true),
 StructField("totalAmount",DoubleType,true)
  ))

  val df = spark.read.option("header", true).schema(OurSchema).csv("/home/kiran/Downloads/yellow_tripdata_2017-01.csv")  
  df.show()
  df.printSchema()
  df.createOrReplaceTempView("DataTable")
  val pickups= spark.sql("SELECT PULocationID,DOLocationID from DataTable")
  pickups.show()
  //pickups.take(5).foreach(println)
 val smallSchema=StructType(Array(
 StructField("SourceDestionations",StringType,true)
  ))

  val tuples=pickups.rdd.map{ line=>
           val p =line.apply(0)
           val d=line.apply(1)
           val a=(p,d).toString()
           Row(a) 
         }
  val sTuples=spark.createDataFrame(tuples,smallSchema)
  sTuples.show()
val count=sTuples.groupBy("SourceDestionations").count().orderBy(desc("count"))
 count.show()
   }    

}