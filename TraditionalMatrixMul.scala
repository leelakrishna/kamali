
object TraditionalMatrixMul{

def main(args:Array[String])
	{
		
		println("enter rows of matrix 1")
		var rows1=scala.io.StdIn.readInt()
		println("enter columns of matrix 1")
		var columns1=scala.io.StdIn.readInt()
		var matrix1=Array.fill(rows1,columns1)(0)
		println("enter matrix 1 varues")
	        for{i <- 0 until rows1; j <- 0 until columns1
		}matrix1(i)(j)=scala.io.StdIn.readInt();
	

		println("enter rows of matrix 2")
		var rows2=scala.io.StdIn.readInt()
		println("enter columns of matrix 2")
		var columns2=scala.io.StdIn.readInt()
		
	
		if(columns1==rows2)
		{
		var matrix2=Array.fill(rows2,columns2)(0)		
		var matrix3=Array.fill(rows1,columns2)(0)
		println("enter matrix 1 varues")
	        for{i <- 0 until rows2; j <- 0 until columns2
		}matrix2(i)(j)=scala.io.StdIn.readInt();
		
		var sum=0		
		for (i<- 0 until rows1) yield {
		for (j<-0 until columns2) yield {
		for(k<- 0 until rows2) yield {
		sum=sum+matrix1(i)(k)*matrix2(k)(j)		
		}
		matrix3(i)(j)=sum
		sum=0		

		}  	

		}
		for{i <- 0 until rows1; j <- 0 until columns2
		}println(s"($i)($j) = ${matrix3(i)(j)}")		
		}
		else{
		println("Multiplication of these matrices is not possible")
		}




		
		
		

	}

 	     
		
}
