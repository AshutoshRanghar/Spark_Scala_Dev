package learning
import scala.io.Source
object DataExtraction_From_File {
 
  def map1(line:String):String=
  {
    var fields=line.split(",")
    
    fields(2)
  }
  
  def main(args:Array[String])
  {
    var list1=Source.fromFile("D:/Big Data Hadoop/test2.txt").getLines().toList
    
    
    list1.foreach(println)
   println(list1.getClass)
    
    var filter1=list1.filter(order=>order.split(",")(1).toInt==4)
    
  println(filter1.toString())
  
  
    var fields=filter1.toString().split(",")
    
    println(fields(2))
  
  
  }
}