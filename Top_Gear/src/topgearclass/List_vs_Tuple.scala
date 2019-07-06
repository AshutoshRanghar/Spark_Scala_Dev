package topgearclass
import org.apache.spark.SparkContext
object List_vs_Tuple {
def main(args:Array[String])
{
  var sc=new SparkContext("local[*]","List_VS_TUPLE")
  
  
  //LIST CAN BE WORKED UpON
  val newList=List("wdwd","DWWDWD","DWDWD")
  
  val tuple1=List((1,"Asdad"),(2,"FWWFW"),(1,"DWWDWW"))
println()
var rdd_tuple=sc.parallelize(tuple1)

var reduceByKey1=rdd_tuple.groupByKey()
    
var collect1=reduceByKey1.collect()
collect1.foreach(println)

}
}