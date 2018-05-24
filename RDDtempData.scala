package com.kamali.exe




import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object RDDtempData{
  
case class TempData(day:Int,doy:Int,month:Int,year:Int,precip:Double,snow:Double,tave:Double,tmax:Double,tmin:Double)

  def main(args:Array[String]):Unit=
  {
    val conf= new SparkConf().setAppName("RDDtempData").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines=sc.textFile("/home/kiran/BigData/MN212142_9392.csv").filter(!_.contains("Day"))
    
    val data=lines.filter(!_.contains(",.,")).map{ line=>
      val p=line.split(",")
      TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble)
    }
    //take returns an Array
   data.take(5).foreach(println)
   
   sc.stop()
      
  }
  
}