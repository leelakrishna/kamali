package com.kamali

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object Dsort{
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // I don't know what this means !!! But still writing it assuming boiler plate code
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Creating a SparkContext
    val sc = new SparkContext("local", "WordCountBetterSorted")   
    
    // Loading each line of my shakeSpeare.txt into an RDD
     val input=sc.textFile("../shakespeare_words.txt")
    
   // Normalizing everything to lowercase because program might assume Upper and lower as different
    val lowercaseWords = input.map(x => x.toLowerCase())
    
    //we are even counting word occurences and later sorting them "alphabetically"
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x,y) => x + y ).sortByKey()
    
    for (result <- wordCounts) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}


