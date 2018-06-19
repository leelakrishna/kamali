package com.kamali.exe
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
//import org.apache.hadoop.fs.s3a.S3AFileSystem 
//import org.apache.hadoop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
object Taxi{
 

  def main(args: Array[String]) {
   
     val sc = new SparkContext("local", "Taxi")
  val data=sc.textFile("/home/kiran/Downloads/yellow_tripdata_2017-01.csv")
  
   //sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId","AKIAJY3KKR5VZK4VKCPQ")
   //sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey","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") // can contain "/"
 
  
    //val data=sc.textFile("s3n://s3.amazonaws.com/nyc-tlc/trip+data/*.csv")
     
     
     val header=data.first()
   
    val Data2=data.filter(row=>row!=header)
    val header2=Data2.first()
   
    val FilteredData=Data2.filter(row=>row!=header2)
     
   val schema=StructType(Array(
  //StructField("vendorID",IntegerType),
 // StructField("pickupDatetime",StringType),
 // StructField("dropDateTime",StringType),
  //StructField("NoOfPass",IntegerType),
  //StructField("tripDistance",DoubleType),
 // StructField("RatecodeID",IntegerType),
 // StructField("StoreFwdFlag",StringType),
  StructField("PULocationID",IntegerType),
 StructField("DOLocationID",IntegerType)
 // StructField("PaymentType",IntegerType),
 // StructField("fare",DoubleType),
 // StructField("extra",DoubleType),
 // StructField("mtaTax",DoubleType),
 // StructField("tip",DoubleType),
 // StructField("tolls",DoubleType),
 // StructField("improveCharge",DoubleType),
 // StructField("totalAmount",DoubleType)
  ))
   
    val rowRDD=FilteredData.map{line=>
    val tempList=line.split(",").toArray
    //val vendorID=tempList.apply(0).toInt
    //val pickupDateTime=tempList.apply(1).toString()
    //val dropDateTime=tempList.apply(2).toString()
    //val NoOfpass=tempList.apply(3).toInt
    //val tripDistance=tempList.apply(4).toDouble
    //val RatecodeID=tempList.apply(5).toInt
    //val StoreFwdFlag=tempList.apply(6).toString()
    var PULocationID=tempList.apply(7)
    var DOLocationID=tempList.apply(8)
    //val PaymentType=tempList.apply(9).toInt
    //val fare=tempList.apply(10).toDouble
    //val extra=tempList.apply(11).toDouble
    //val mtaTax=tempList.apply(12).toDouble
    //val tip=tempList.apply(13).toDouble
    //val tolls=tempList.apply(14).toDouble
    //val improveCharge=tempList.apply(15).toDouble
    //val totalAmount=tempList.apply(16).toDouble
    /*(vendorID,pickupDateTime,dropDateTime,NoOfpass,tripDistance,
        RatecodeID,StoreFwdFlag,PULocationID,DOLocationID,
        PaymentType,fare,extra,mtaTax,tip,tolls,improveCharge,
        totalAmount)*/
    
    (PULocationID,DOLocationID)
  }
val newRDD=rowRDD.filter(x=>( x._1!=x._2)) 
val tupledRDD=newRDD.map(x=>(x,1))
 
  val countRDD=tupledRDD.reduceByKey((x,y)=>x+y)
  val flippedRDD=countRDD.map(x=>(x._2,x._1))
  val sortedRDD=flippedRDD.sortByKey()
  val topNelements=sortedRDD.top(10)
  topNelements.foreach(println)
  
  /* val pickups=FilteredData.map{ line=>
    val tempList=line.split(",").toArray
    var PULocationID=tempList.apply(7).toInt
    val k =Vectors.dense((PULocationID))
    (k)
    }
 pickups.cache()
val numClusters = 2
val numIterations = 20
val clusters = KMeans.train(pickups,numClusters,numIterations) 
println(clusters.computeCost(pickups))
*/
 
 }
    

}                    