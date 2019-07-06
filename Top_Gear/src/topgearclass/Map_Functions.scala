package topgearclass
import org.apache.spark.SparkContext

object Map_Functions {

  def main(args:Array[String])
  {
    var s:String="I am a good boy"
  var b1=s.flatMap(s=>s+"1")
  var b2=s.map(s=>s+"1")
  
    println("Flat Map\n"+b1)
    println("\nMap"+b2)
 
  var l1=List("I","am","a","good")
  
  var l2:String="I am a good boy and i can swim"
  var g =l1.mkString(" ")
  var c1=l1.flatMap((x=>(x+"1")))
  
  var c2=l1.map(s=>s+"1")
  
  var map1=l2.map(x=>(x,1))
  
  
  println("Flat Map on List"+c1)
  println("\nMap on List"+c2)
  }
}