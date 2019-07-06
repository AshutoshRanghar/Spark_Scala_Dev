package topgearclass
import org.apache.spark.SparkContext
object Extract_Date {
  
  def main(args:Array[String])
  {
    
    var sc=new SparkContext("local[*]","Extract_Date")
    
    var rdd1=sc.textFile("../dg_raju/orders_data.txt")
//    var str1=rdd1.map(x=>x.split(",")(1).substring(0,10).replace("-","").toInt)
 
    var rdd2=sc.textFile("../dg_raju/order_item.txt")
  
    var order_data_map=rdd1.map(x=>x.split(",")).map(x=>(x(2),x(3)))
    
    var order_item_map=rdd2.map(x=>x.split(",")).map(x=>(x(2),x(4)))
 
 
    
    var sample=order_data_map.take(10).foreach(println);
    //inner join 
    var joins=order_data_map.join(order_item_map)
    joins.take(10).foreach(println)
        
    //Left outer join
    
    var lef_outer=order_data_map.leftOuterJoin(order_item_map)
    var t=lef_outer.filter(x=>x._2._2!="None")
    t.take(10).foreach(println)
    
    
    
  }
}