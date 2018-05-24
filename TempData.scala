package com.kamali.exe

import scala.io.Source.fromFile

case class TempData(day:Int,doy:Int,month:Int,year:Int,precip:Double,snow:Double,tave:Double,tmax:Double,tmin:Double)
object TempData{
  
  def main(args:Array[String]):Unit={
    
    val source=scala.io.Source.fromFile("/home/kiran/BigData/MN212142_9392.csv")
    val lines=source.getLines().drop(1)//dropping the first row which has column names
   
    val data=lines.filterNot(_.contains(",.,")).map{ line=>
      val p=line.split(",")
      TempData(p(0).toInt,p(1).toInt,p(2).toInt,p(4).toInt,p(5).toDouble,p(6).toDouble,p(7).toDouble,p(8).toDouble,p(9).toDouble)
    }.toList
   
    source.close()
    println(data.length)
    data.take(5).foreach(println)
    
    //printing highest temp date
    
   /* val maxTemp=data.map(x=>x.tmax).max
    println(maxTemp)
    val hotdays=data.filter(x=>x.tmax==maxTemp)
    hotdays.take(5).foreach(println)
    */
    //or we can even do this with maxBy function
    println("hot was at")
    val hotday=data.maxBy(x=>x.tmax)
    println(hotday)
    val rainyCount=data.count(x=>x.precip>=1.0)
    println(rainyCount)
  }
}
