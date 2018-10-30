package learning
import org.apache.spark.SparkContext
object Retail_Db_Assignment {
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","Assignment_RetailDb")
   
    var main_rdd=sc.textFile("../dg_raju/orders_data.txt")
    //Filtering out the COMPLETE and CLOSED
    var filter_main=main_rdd.filter(order=>order.split(",")(3) =="COMPLETE" || order.split(",")(3)=="CLOSED")
   
    
    
   // filter_main.take(5).foreach(println)
  
    //Setting Only Date as the key Column
    
    var map_main=filter_main.map(product => (product.split(",")(1).split(",")(0).split(" ")(0), product))
 
    //map_main.foreach(println)
    
    //Sorting by the key (DATE)to get the ascending order from date 
    
    var sort_main_rdd=map_main.sortByKey()
    sort_main_rdd.take(100).foreach(println)
    
    //Creating a map join of rdds
    //Mapper Rdd of orders
    
    
    val ordersMap = filter_main.
  map(order => (order.split(",")(0).toInt, order.split(",")(1)))
    
   var order_item_rdd=sc.textFile("../dg_raju/order_item.txt")
    //Creating map RDD of (orderId,order_item)
   
   val orderItemsMap = order_item_rdd.
  map(oi => (oi.split(",")(1).toInt,(oi.split(",")(2).toInt, oi.split(",")(4).toFloat)))
   
  ordersMap.take(5).foreach(println)  
  
  orderItemsMap.take(5).foreach(println)
  
  
    //Creating Join over the rdd
    var join_rdd=ordersMap.join(orderItemsMap)
    join_rdd.take(5).foreach(println)
    
  //((order_id),(order_date(product_id,order_item_subtotal)))  
    //Convert to 
    
    //(order date,product_id),order_item_subtotal
    var convert_join=join_rdd.map(x=>((x._2._1,x._2._2._1),(x._2._2._2)))
    convert_join.take(5).foreach(println)
    
    //As we need to aggregate over DAILYYYYY  PRODUCT ID
    //Means DAILY and PRODUCT ID WOULD BE THE KEY fOR THE REDUCTION OF The Data
    //FINDING AGGREGATE OVER DAILY AND PRODUCT ID
  
    var daily_subtotal=convert_join.reduceByKey((x,y)=>(x+y))
    daily_subtotal.take(10).foreach(println)
    
    
    //FINDING MAXIMUM SUBTOTAL OF DAILYY AND PRODUCT ID as group by over //(order date,product_id),order_item_subtotal
    var max_group_by_join=convert_join.groupByKey().map(x=>(x._1,x._2.max))
    max_group_by_join.take(10).foreach(println)
        
    
    //MATCHING WITH THE PRODUCT FEED..
    
    var product_rdd=sc.textFile("../dg_raju/products/main_product")
    
    
    
    
  }
}