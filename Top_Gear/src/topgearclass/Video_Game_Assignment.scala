package topgearclass
import org.apache.spark.SparkContext
object Video_Game_Assignment {
  
  def extract_platforms(line:String)=
  {
   var fields=line.split(",")
   (fields(2))
  }
  
  def v_g_sales(line:String)=
  {
   var fields=line.split(",")
   (fields(2),fields(10))
    
  }
  
  
  def main(args:Array[String])
  {
  var sc=new SparkContext("local[*]","Video_Game_Assigment")
  
 
  
   var main_rdd_temp=sc.textFile("../v_g/vgsales.csv")
  //Removing Header
   var removal=(main_rdd_temp.take(1)(0)).toString()
   
   var main_rdd =main_rdd_temp.filter(x=>x!=removal)
  
   var games_Rdd=main_rdd.map(x=>x.split(",")).map(x=>x(2)).distinct().count()
   println(games_Rdd)
  
   var platform=main_rdd.map(x=>x.split(",")).map(x=>x(2)).map(x=>(x,1))
  
   var reducer=platform.reduceByKey((x,y)=>(x+y))
  
   var collector=reducer.collect()
  
   collector.foreach(println)
      
   //Total Sales of each video game.
  
   var sample1=main_rdd.map(x=>x.split(",")).map(x=>(x(2),x(10).toDouble))
  
   //var map_sales=sample1.map(x=>(x._1,x._2))
   
   var reducer1=sample1.reduceByKey((x,y)=>(x+y))
   println("Sales")
   //var exchange=reducer.map(x=>(x._2,x._1))
  
   var sorter=reducer1.sortByKey(true)
   sorter.foreach(println)
   //Max value of each platform
  
   var sample2=main_rdd.map(x=>x.split(",")).map(x=>(x(2),x(10).toDouble))
  
   var max2=sample2.groupByKey().map(x=>(x._1,x._2.toList.max))
   println("This is the SAMPLE 2")  
   max2.foreach(println)
   
   //Top 10 Sales of the consoles
   
   var top_10=sample2.map( x => (x._2, x._1) )
   var sorter1=top_10.sortByKey(false)
   val sorter2=sorter1.map(x=>(x._1,x._2))
    
   sorter2.take(10).foreach(println)
   
  
  }
}