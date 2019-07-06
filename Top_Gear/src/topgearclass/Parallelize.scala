package topgearclass
import org.apache.spark.SparkContext
object Parallelize {
  def main(args:Array[String])
  {
 
    val sc=new SparkContext("local[*]","Ash")
 
    var s=(0 to 20).toList
    
   val rdd1=sc.parallelize(s)
  rdd1.foreach(println)
   var a=rdd1.collect();
  
    
    a.foreach(println)
  //Loading multiple files in an rdd
    
    var lines=sc.textFile("../temp/u1.user")
    
    var gen=lines.map(x=>x.size)
    
    print(gen)
    
    var length=lines.count()
    print(length)
    
  }
}