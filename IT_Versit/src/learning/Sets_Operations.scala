package learning
import org.apache.spark.SparkContext

object Sets_Operations {
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","Map_Scala")
    var main_rdd=sc.textFile("../dg_raju/orders_data.txt")
   println(main_rdd.first()) 
  }
}