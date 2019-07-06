package topgearclass
import org.apache.spark.SparkContext
object Joins_In_Spark {
  def main(args:Array[String])
  {
    var first=List((1,"Raj"),(2,"Mahesh"),(3,"Chandra"),(4,"Rudra"),(5,"Samridra"))
    var second=List((1,"Industry"),(3,"Choclate"),(4,"Icecream"))
    var sc=new SparkContext("local[*]","Joins")
    
    var first_rdd=sc.parallelize(first)
    var second_rdd=sc.parallelize(second)

    //Normal Join
    
    var inner_join_rdd=first_rdd.join(second_rdd)
    
    inner_join_rdd.foreach(println)
    
    //Left Outer Join
    
    var left_outer_join=first_rdd.leftOuterJoin(second_rdd)
    left_outer_join.foreach(println)    
  
    //Filtering out the None
  
    var filter1=left_outer_join.filter(x=>x._2._2==None)
    var collector=filter1.collect()
    
  collector.foreach(println)
    
  }
}