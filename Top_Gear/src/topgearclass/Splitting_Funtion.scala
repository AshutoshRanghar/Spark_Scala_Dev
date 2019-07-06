package topgearclass

import org.apache.spark.SparkContext
object Splitting_Funtion {
  def main(args:Array[String])
  {
    
    var sc=new SparkContext("local[*]","Ash")
  val list1=List("1,Ash,doon,","2 ,Sneha, mad")
  
  var rdd1=sc.parallelize((list1) )
  
  println(list1(0))
  
  var break_name=list1.map(x=>x.split(",")(1))  
  
      println(break_name)
  }
}