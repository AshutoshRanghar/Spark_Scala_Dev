package learning

import org.apache.spark.SparkContext
object Learn_Max_and_min {
  def main(args:Array[String])
  {
     // ORDER ITEMS REDUCE BY REDUCE FUNCTION
   
    var sc=new SparkContext("local[*]","Map_Scala")
    var main_rdd=sc.textFile("../dg_raju/orders_data.txt")
   
    var main_rdd2=sc.textFile("../dg_raju/order_item.txt")
   
   var map_total=main_rdd2.map(x=>x.split(",")(4).toFloat)
   
   var total_revenue=map_total.reduce((revenue,total)=>revenue+total)
   println(total_revenue)
   
   //GROUP BY KEY GETTING FILE
   
   var group_rdd=main_rdd.map(x=>x.split(",")).map(x=>(x(3),x(2).toDouble))
    //GETTING mIn rEVENUe by reduce by key 
   
   var min_rev=group_rdd.reduceByKey((min,revenue)=>if(min>revenue)  revenue else min)
   
   min_rev.foreach(println)
    
   //Getting min revenue by group by key
   var group_min=group_rdd.groupByKey().map(x=>(x._1,x._2.toList.min))
  println("THIS IS THE MIN VALUE TEsTING")
  group_min.foreach(println)
  
  
   
  
  }
}