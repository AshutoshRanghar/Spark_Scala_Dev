package learning
import org.apache.spark.SparkContext

object Sets_Operations {
  def main(args:Array[String])
  {
    var sc=new SparkContext("local[*]","Map_Scala")
    var main_rdd=sc.textFile("../dg_raju/orders_data.txt")
  //Just checking the mapping of the data
    var mapping_first= main_rdd.map(x=>x.split(",")(1))
    
    var get_year=mapping_first.map(x=>x.split("-")(0))
    get_year.take(5).foreach(println)
  
  //Filter_2013 Auguest and September data and map out the order IDs of the customers
    var august_customer=main_rdd.filter(x=>x.split(",")(1).contains("2013-08")).map(august_customer=>august_customer.split(",")(2))
    var september_customer=main_rdd.filter(x=>x.split(",")(1).contains("2013-09")).map(sept_customer=>sept_customer.split(",")(2))
   august_customer.take(5).foreach(println)
   september_customer.take(5).foreach(println)
   
   //Check the count
   println("The count of august customer data"+august_customer.count())
   println("The count of Sept customer data"+september_customer.count())
   
   
   
  // Find Intersection of the data finding common customers in august 2013 and 2013 September
  //Intersection is DISTINCTTTTTTTTTTTTTTT common elements of data in a set
   var int_aug_sep=august_customer.intersection(september_customer)
   
   //int_aug_sep.foreach(println)
    println("The count of Union between Aug and Sep "+int_aug_sep.count())
 //Find the Union of data finding between customers in Aug and sept
 //Union is common and uncommon data as well also not distinct
  
    var union_aug_sep=august_customer.union(september_customer)
   println("The count of Union between Aug and Sep "+union_aug_sep.count())
    
   //Using left outer join only for the common elements from the left side  
   
   var pair_august_data=august_customer.map(x=>(x,1))
   
   var pair_sep_data=september_customer.map(x=>(x,1))
   
   var left_aug_sep=pair_august_data.leftOuterJoin(pair_sep_data)
   
   println(left_aug_sep.distinct().foreach(println))
  
  /*
  It will give sample results 
  
  (1137,(1,None))
(7778,(1,None))
(2615,(1,None))
(9274,(1,Some(1)))
(4659,(1,Some(1)))
(10096,(1,Some(1)))

That means the match and unmatch None means no match and some means there is the other data set matching
  
Also we can filter out the unmatching records
  */
   
   var match_left_aug_sep=left_aug_sep.filter(rec=>rec._2._2==None)
   match_left_aug_sep.foreach(println)
   
   
  }
}