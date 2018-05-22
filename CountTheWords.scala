package com.kamali.exe
import scala.io._
import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object CountTheWords{
  
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
    
    
    countRDD.foreach(println)    
    //printing the RDD
    
    
  }
  
}