package com.kamali.exercises;

import org.apache.spark._

import org.apache.spark.SparkContext._
import org.apache.log4j._

object Join2Texts{
  
  
  def main(args:Array[String])
  {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   val sc=new SparkContext("local[*]","Join2Texts")
   
    val input1=sc.textFile("/home/kiran/shakespeare_words.txt")
    
    val input2=sc.textFile("/home/kiran/transactions.txt")
    
    val output=input1.union(input2)
    output.collect
    
    output.saveAsTextFile("/home/kiran/JoinedTexts.txt")
    
    
    
  }
  
}
