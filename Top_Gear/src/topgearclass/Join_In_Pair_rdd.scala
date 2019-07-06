package topgearclass
import org.apache.spark.SparkContext
  
object Join_In_Pair_rdd {
  def main(args:Array[String])
  {
    var first=List((1,  ("Raj","India")),
        (2,("Mahesh","France"))
        
        ,(3,("Chandra","Japan")),
        (4,("Rudra","Canada"))
        ,(5,("Samridra","Detroit")))
        
    var second=List((1,"Industry"),(3,"Choclate"),(4,"Icecream"))
    var sc=new SparkContext("local[*]","Joins")
 
     var first_rdd=sc.parallelize(first)
     var second_rdd=sc.parallelize(second)

      //Normal Join
    
    var inner_join_rdd=first_rdd.join(second_rdd).collect()
    
    inner_join_rdd.foreach(println)
    //Extracting Indexing Tuple values
    
    var distinct_industry=inner_join_rdd.map(x=>x._2._2).distinct
    distinct_industry.foreach(println)
    
    
    //Left Outer Join
    
    var left_outer_join=first_rdd.leftOuterJoin(second_rdd).collect()
    left_outer_join.foreach(println)    
 
    
  }  
  

}