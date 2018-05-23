package com.kamali.exe
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

 
object disSort{
 

  def main(args: Array[String]) {
   

    Logger.getLogger("org").setLevel(Level.ERROR)
  
    val sc = new SparkContext("local", "disSort")   
    
   
     val input=sc.textFile("../shakespeare_words.txt")

   
    val words = input.flatMap(x => x.split("\\W+"))
    
   
    val lowercaseWords = words.map(x => x.toLowerCase())
    
   
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    val wordCountsSorted = wordCounts.map( x => (x._1, x._2) ).sortByKey()
    

    for (result <- wordCountsSorted) {
      //val count = result._1
      val word = result._1
      println(s"$word")
    }
    
  }
  
}

