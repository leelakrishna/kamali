package com.kamali.exe
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.hadoop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions._
//import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.regression.LinearRegression
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark._
object LR{
 
 def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val sc =new SparkContext("local[*]","TaxiLogistic")
    
    val data=sc.textFile("/home/kiran/Desktop/csvs/yellow_tripdata_2017-01.csv")
    
    val header=data.first()
    val Data2=data.filter(row=>row!=header)
    val header2=Data2.first()
    val FilteredData=Data2.filter(row=>row!=header2)
    val sample=sc.makeRDD(FilteredData.takeSample(false, 100))
    val prices=sample.map{ line=>
                                val tempList=line.split(",")
                                val q=tempList.apply(16).toDouble
                                (q)  
                               }
  val distinct=prices.distinct()
 
  val map=distinct.zipWithIndex().collectAsMap() //mapper function
  val rmap=map.map(_.swap) //demapper function.
  
  val dd=sample.map{line =>
    
  val tempList=line.split(",")
  
    val a=tempList.apply(0).toInt
   //val b=tempList.apply(1)
   //val c=tempList.apply(2)
    val d=tempList.apply(3).toInt
    val e=tempList.apply(4).toDouble
    val f=tempList.apply(5).toInt
  //val g=tempList.apply(6)
    val h=tempList.apply(7).toInt
    val i=tempList.apply(8).toInt
    val j=tempList.apply(9).toInt
    val k=tempList.apply(10).toDouble
    val l=tempList.apply(11).toDouble
    val m=tempList.apply(12).toDouble
    val n=tempList.apply(13).toDouble
    val o=tempList.apply(14).toDouble
    val p=tempList.apply(15).toDouble
    val q=tempList.apply(16).toDouble
    
    LabeledPoint(map(q),Vectors.dense(a,d,e,f,h,i,j,k,l,m,n,o,p))
  }
  
    dd.cache()
    val model = new LogisticRegressionWithLBFGS().setNumClasses(11101).run(dd)
////////////////////
    val Data=sc.textFile("/home/kiran/Desktop/csvs/yellow_tripdata_2017-02.csv")
    
    val Header=data.first()
    val data2=data.filter(row=>row!=header)
    val Header2=data2.first()
    val filteredData=data2.filter(row=>row!=header2)
    val Sample=sc.makeRDD(filteredData.takeSample(false, 100))
     
  val predictionsAndLabels=sample.map{line =>
    
  val tempList=line.split(",")
  
    val a=tempList.apply(0).toInt
   //val b=tempList.apply(1)
   //val c=tempList.apply(2)
    val d=tempList.apply(3).toInt
    val e=tempList.apply(4).toDouble
    val f=tempList.apply(5).toInt
  //val g=tempList.apply(6)
    val h=tempList.apply(7).toInt
    val i=tempList.apply(8).toInt
    val j=tempList.apply(9).toInt
    val k=tempList.apply(10).toDouble
    val l=tempList.apply(11).toDouble
    val m=tempList.apply(12).toDouble
    val n=tempList.apply(13).toDouble
    val o=tempList.apply(14).toDouble
    val p=tempList.apply(15).toDouble
    val q=tempList.apply(16).toDouble
    
   (map(q).toDouble, model.predict(Vectors.dense(a,d,e,f,h,i,j,k,l,m,n,o,p)))
  }
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
 }
}
