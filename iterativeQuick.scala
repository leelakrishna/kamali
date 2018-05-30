import scala.util.matching.Regex
import scala.io._
import scala.util.matching._
object iterativeQuick{
  
  def main(args:Array[String]):Unit=
  {
    
    val source=scala.io.Source.fromFile("/home/kiran/Downloads/positive-words.txt")
    val lines=source.getLines().filter(!_.isEmpty()).toList
    val refinedlines=lines.filter(!_.contains(",")).filter(!_.contains(".")).filter(!_.contains("'")).filter(!_.contains(";")).filter(!_.contains("?")).toArray
    source.close()
    
 val sorted=QuickSort(refinedlines,0,refinedlines.length-1)

/*for ( k <-0 to sorted.length-1)
   {
     println(sorted.apply(k))
   }
 
  
*/
}
def QuickSort(l1:Array[String],l:Int,h:Int):Array[String]={

	if(l<h)
	{
	var p=partition(l1,l,h)
	QuickSort(l1,l,p-1)
	QuickSort(l1,p+1,h)

	}

	return l1
	
	}

def partition(l1:Array[String], l:Int , h:Int):Int=
	{
	var x=l1.apply(h)
	var i=l-1
	
     
		for( j <- l to h-1)
			{
			if (l1.apply(j).compareTo(x)>0)
				{
				 i=i+1
				 var temp=l1(i)
				 l1(i)=l1(j)
				 l1(j)=temp
				 
				}
			}
			var k=i+1
			(k)
		
	
	}


}
