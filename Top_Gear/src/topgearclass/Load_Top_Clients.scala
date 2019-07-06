package topgearclass
import org.apache.spark.SparkContext
object Load_Top_Clients {
 
  def parseLine(line:String):String={
    
    var fields=line.split(",")
    var id=fields(0).toString()
    return id
  }
  
  
  def country(line:String):String={
    var fields=line.split(",")
    var id=fields(7).toString()
    
    if (id=="United Kingdom")
    return "TRUE"
    
    else
      
      return "FALSE"
  }
  
  def line_gen(line:String){
    var fields=line.split(",")
      val name=fields(1).toString()
      val age = fields(2)
      val numFriends = fields(3)
      // Create a tuple that is our result.
      (age, numFriends)
  }
  def main(args:Array[String])
  {
    val sc=new SparkContext("local[*]","Top Program")
    
    val rdd_elements=sc.textFile("../Online Retail.csv")
  // var rdd1=rdd_elements.map(parseLine)
    
   var rdd2=rdd_elements.map(country)
  
    val collect_rdd=rdd2.collect()
 collect_rdd.foreach(println)
    var rdd1=rdd_elements.map(x=>x.split(",")(7).contains("United Kingdom"))
  val collect=rdd1.collect()
 collect.foreach(println)
   
  val rdd3=rdd_elements.map(line_gen)
    
    val collect_rdd4=rdd_elements.map(x=>x.substring(0,6)).take(10)
    val collector=collect_rdd4;
   collector.foreach(println)
    
    
    val top_man=rdd_elements.map(x=>x.replaceAll("United Kingdom", "Angrez")).take(10)
    val collector1=top_man
    collector1.foreach(println)
    
    
    
    
   
   
   
   
  }
}