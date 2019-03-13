package learning

import org.apache.spark.SparkContext
object Log_File_Analyses {
  def main(args:Array[String])
  {
    
    var sc=new SparkContext("local[*]","Log File Analysis")
  var main_rdd=sc.textFile("../access_logs.txt")
  
 var spliting =main_rdd.map(x=>x.split("/")).map(x=>(x(0).substring(0,14).trim(),x(1)))
 
 var get_months=spliting.map(x=>x._2)
 
 var distinct=get_months.distinct()
distinct.take(10).foreach(println)
 
  var get=spliting.take(10)
  get.foreach(println)
  
 
  
    
    
    
    
  }
}