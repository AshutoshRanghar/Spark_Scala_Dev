package learning

import org.apache.spark.SparkContext
object Aggregate_By_Key_Practise {
  
  def main(args:Array[String])
  {
  //Aggregate By Key and Reduce By Key have combiner and not group by key.  
  //Aggregate by key is used by when logic between intermediate values and logic combining intermeidate values is different thats why 
   //we have two functions one for logic writing other for combining the data.
       var sc=new SparkContext("local[*]","Map_Scala")
 
    var main_rdd2=sc.textFile("../dg_raju/order_item.txt")
   
    
    
// Compute revenue and number of items for each order using reduceByKey
    
    
 var map_order_item= main_rdd2.map(orderItem => (orderItem.split(",")(1).toInt, (orderItem.split(",")(4).toFloat, 1)))
  var map_order_item_agg= main_rdd2.map(orderItem => (orderItem.split(",")(1).toInt, (orderItem.split(",")(4).toFloat)))
 
 var reduce_order_item=map_order_item.reduceByKey((total, element) => (total._1 + element._1, total._2 + element._2)).take(100).foreach(println)  
    
  
  
  
  

   var map_order= map_order_item_agg.
  aggregateByKey((0.0, 0))(
    (iTotal, oisubtotal) => (iTotal._1 + oisubtotal, iTotal._2 + 1),
    (fTotal, iTotal) => (fTotal._1 + iTotal._1, fTotal._2 + iTotal._2)
    )
   println("AGG BY KEY")
   map_order.take(2).foreach(println)
   
    //(order_id,(order_revenue,max_order_subtotal))
  }
  //Using reduce by key

  

}