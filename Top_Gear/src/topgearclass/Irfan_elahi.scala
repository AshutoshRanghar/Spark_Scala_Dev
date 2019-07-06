package topgearclass
import org.apache.spark.SparkContext
object Irfan_elahi {
  
  def mapp(line:String)=
  {
  var fields=line.split(",")
  (fields(0),fields(3),fields(7))
  
  }
  
  
  
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","filter_transf")
    
    var v=('a' to 'z').toList
    
    var rdd1=sc.parallelize(v)
    var string1=rdd1.toString()
  var getrdd=rdd1.flatMap(x=>x.toString().split(" "))
    
  var count=getrdd.countByValue()
  count.foreach(println)
  
    /*
    var ab=sc.textFile("../Online Retail.csv")
    
    var rdd1=ab.take(6);
  // rdd1.foreach(println)
    
   var mapper=ab.map(mapp)
   
   var notuk=mapper.filter(x=>x._3!="United Kingdom")
   
   notuk.take(20).foreach(println)
   var c2=ab.map(x=>x.split(",")(7)!="United Kingdom").take(5)
   
   //SPAIN FILTER
   var spain=mapper.filter(x=>x._3=="Spain")
   
   var counter=spain.count()
   
   println(counter) 
   
   // ReMOVIng HEADERS 
   
   var guess=ab.take(1).toString()
   println("EGEGEGEGEGGEGEGE",guess)
   guess.foreach(println)
   
   var removal=(ab.take(1))(0).toString()
  
   var check=ab.take(1).toString()
   check.foreach(println)
   println("THIS IS CHEKKKK",check)
   
   
   var rem_head= ab.filter(x=>x!=removal)
   rem_head.take(6).foreach(println)
  */ 
   
 //  var d1=c1.take(10)
   
 //d1.foreach(println)
 
 //c2.foreach(println)
   
   
  }
}