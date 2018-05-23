package com.kamali.exe
import scala.util.matching.Regex
import scala.io._
import scala.util.matching._
object StandAloneMergeSort{
  
  def main(args:Array[String]):Unit=
  {
    
    val source=scala.io.Source.fromFile("/home/kiran/normalWords.txt")
    val lines=source.getLines().filter(!_.isEmpty()).toList
    val refinedlines=lines.filter(!_.contains(",")).filter(!_.contains(".")).filter(!_.contains("'")).filter(!_.contains(";")).filter(!_.contains("?")).toList
    source.close()
    
 val sorted=mergeSort(refinedlines)
    
   for ( k <-0 to refinedlines.length-1)
   {
     println(sorted.apply(k))
   }
  }
def merge(l1:List[String],l2:List[String]):List[String]= (l1,l2) match{

	case(Nil,_)=>l2
	case(_,Nil)=>l1
	case(h1::t1, h2::t2)=>
		if(h1<h2) h1::merge(t1,l2)
		else
		    h2::merge(l1,t2)
}
def mergeSort(lst:List[String]) :List[String]=lst match{

	case Nil=>lst
	case h::Nil=>lst
	case _ =>
			val (l1,l2)=lst.splitAt(lst.length/2)
			merge(mergeSort(l1),mergeSort(l2))


  }

}
