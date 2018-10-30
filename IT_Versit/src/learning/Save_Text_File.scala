package learning

import org.apache.spark.SparkContext
  
object Save_Text_File {
  
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","Save as Text  file")
  
    var main_rdd=sc.textFile("../dg_raju/orders_data.txt")
    
    var map_main_rdd=main_rdd.map(x=>(x.split(",")(3),1))
   var reduction=map_main_rdd.reduceByKey((x,y)=>(x+y)) 
  reduction.take(5).foreach(println)
  reduction.saveAsTextFile(("../dg_raju/order_by_Statuscodec"),classOf[org.apache.hadoop.io.compress.SnappyCodec])
  
  }
  
}