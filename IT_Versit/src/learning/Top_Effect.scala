package learning
import scala.io.Source
object Top_Effect {
  def main(args:Array[String])
  {
    var ab=Source.fromFile("D:/Big Data Hadoop/test1.txt")
    println(ab)
    println(ab.getClass())
    var b=ab.toList
    
    var list1=Source.fromFile("D:/Big Data Hadoop/test2.txt").getLines.toList
    var ob=list1.filter(x=>x.split(",")(1).toInt==4)
    ob.foreach(println)
 
    
    
    
     }
}