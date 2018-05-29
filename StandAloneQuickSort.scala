
import scala.util.matching.Regex
import scala.io._
import scala.util.matching._
object StandAloneQuickSort{
  
  def main(args:Array[String]):Unit=
  {
    
    val source=scala.io.Source.fromFile("/home/kiran/shakespeare_words.txt")
    val lines=source.getLines().filter(!_.isEmpty()).toList
    val refinedlines=lines.filter(!_.contains(",")).filter(!_.contains(".")).filter(!_.contains("'")).filter(!_.contains(";")).filter(!_.contains("?")).toList
    source.close()
    
 val sorted=QuickSort(refinedlines)
    
   for ( k <-0 to refinedlines.length-1)
   {
     println(sorted.apply(k))
   }
  }

def QuickSort(lst:List[String]):List[String]=lst match{
	
	case Nil =>lst
	case h::Nil=>lst
	case pivot:: t=>
		val(less,greater)=t.partition(_< pivot)
		QuickSort(less)::: (pivot::QuickSort(greater))

  }

}

