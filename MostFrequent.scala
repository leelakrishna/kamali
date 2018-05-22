package com.kamali.exe
import scala.io._
import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object MostFrequent{
  
  def main(args:Array[String]):Unit=
  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    
    val sc =new SparkContext("local[*]","wordCount")
    
    val RDD=sc.textFile("/home/kiran/shakespeare_words.txt")
    //read the text file
    
    val cleanedRDD=RDD.flatMap(x => x.split("\\W+"))
    //eliminating special characters and taking only words
    val lowerCase=cleanedRDD.map(x=>x.toLowerCase())
    //converting every word into lower case because system must not consider twoCases as different words
    
    val tupledRDD=lowerCase.map(x=>(x,1))
    //making a tuple of word and by defualy 1 as occurence
    
    val countRDD=tupledRDD.reduceByKey((x,y)=>x+y)
    //adding same words so occurences will e added.
    
    
    
    //printing the RDD
    val flippedRDD=countRDD.map(x=>(x._2,x._1))

    val sortedRDD=flippedRDD.sortByKey()
    
 
    sortedRDD.collect()
   
    
 val topNelements=sortedRDD.top(10)
 //top function takes records from last in the RDD, top(10) takes last 10 elements with descending order
    
  topNelements.foreach(println)
   
    
    
    
  }
  
}