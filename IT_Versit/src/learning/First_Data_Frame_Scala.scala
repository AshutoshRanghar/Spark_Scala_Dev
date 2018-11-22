package learning
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object First_Data_Frame_Scala {
  def main(args:Array[String])
  {
    
 /*   var conf = new SparkConf();
conf.setAppName("Spark MultipleContest Test");
conf.set("spark.driver.allowMultipleContexts", "true");
conf.setMaster("local");
nOt working
    
    */
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      
      var first_df=spark.read.csv("../dg_raju/orders_data.txt")
      first_df.take(10).foreach(println)
      val columnsRenamed = Seq("order_id", "order_date", "order_customer_id", "order_status")
      val explodeDF = first_df.toDF(columnsRenamed: _*)
      
     // explodeDF.take(5).foreach(println)
     explodeDF.schema.fields.foreach(println)
      
  explodeDF.createOrReplaceTempView("orders")
  explodeDF.sqlContext.sql("select order_status,count(1) from orders group by order_status limit 10").show()
  explodeDF.printSchema()
    
 //SECOND PRODUCT TABLE
  
  var second_df=spark.read.csv("../dg_raju/products/main_products")
 // second_df.take(5).foreach(println)
 
 
 val columnsRenamed1 = Seq("a", "b", "c", "d","e","f")
 var second_rename=second_df.toDF(columnsRenamed1:_*)     
  second_rename.take(6).foreach(println)
  
  second_rename.printSchema()
 
  second_rename.createOrReplaceTempView("products")
 
  /*REMOVE COLUMN IN DATAFRAME*/
 var remove= second_rename.select("a","b","c","f").drop("d","e").limit(10).show()
  
 
  
  second_rename.sqlContext.sql("select order_date,c,sum(e) daily_rev from products inner join orders on order_id=b where order_status "+
"in ('COMPLETE','CLOSED')  group by order_date,c order by daily_rev desc limit 10").show()
  
  }
}