package learning
import org.apache.spark.SparkContext
import scala.math.Ordering

object Ranking_Scala {
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","Ranking DataSet")
    
    var main_rdd=sc.textFile("../dg_raju/products/main_products")//.take(684)
  //  var main_rdd=sc.parallelize(main_rdd1)
  
  var initial_filter=main_rdd.filter(x=>x.split(",")(4)!="")  
  
   var map_product=initial_filter.map(x=>x.split(",")).map(x=>
     (x(1),(x(4).toDouble,1)))
   //  if(x(1)=="" )(x(1),x(4).toDouble)else ("NA",0.000))
  
      
     var reduce_product=map_product.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
   
   var exchange_product=reduce_product.map((x)=>(x._2,x._1))
   
   var sorting=exchange_product.sortByKey(false)
   
   var rechange=sorting.map((x)=>(x._2,x._1))
   var top_10=rechange.take(10).foreach(println)
   // Ordered RDD scala
   //Ordering the entire on behalf of the amount.
   initial_filter.takeOrdered(10)(Ordering[Float].reverse.on(x=>x.split(",")(4).toFloat)).foreach(println)
   //var top_10_rank=rank_rdd.take(10)
   
  }
}