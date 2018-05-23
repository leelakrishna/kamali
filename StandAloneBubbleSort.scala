package com.kamali.exe
import scala.util.matching.Regex

import scala.io._
import scala.util.matching._
object StandAloneSort {
  
  def main(args:Array[String]):Unit=
  {
    
    val source=scala.io.Source.fromFile("/home/kiran/shakespeare_words.txt")
    
    
 
    val lines=source.getLines().filter(!_.isEmpty()).toArray
    val refinedlines=lines.filter(!_.contains(",")).filter(!_.contains(".")).filter(!_.contains("'")).filter(!_.contains(";")).filter(!_.contains("?")).toArray
    
    
  /* for ( i <-0 to refinedlines.length-1)
   {
     println(refinedlines.apply(i))
   }
   */

    for ( i <-0 to refinedlines.length-1)
    {
      for(j<-0 to refinedlines.length-1)
        if(refinedlines.apply(i).compareTo(refinedlines.apply(j))>0)
            {
            val temp= refinedlines.apply(i)
            refinedlines(i)=refinedlines(j)
            refinedlines(j)=temp
          
            }
    }
    
   for ( k <-0 to refinedlines.length-1)
   {
     println(refinedlines.apply(k))
   }
  }
  
}
