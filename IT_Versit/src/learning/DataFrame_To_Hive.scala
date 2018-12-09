package learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object DataFrame_To_Hive {
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
      spark.sqlContext.sql("Create database ash_db")
      spark.sqlContext.sql("Use ash_db")
      spark.sqlContext.sql("CREATE TABLE ash_db.daily_revenue " +
"(order_date string, product_name string, daily_revenue_per_product float) " +
"STORED AS orc")

//var products_df1=spark.read.csv("../dg_raju/products/main_products")
//Changing Data frame attributes.
 //val columnsRenamed1 = Seq("a", "b", "c", "d","e","f")
//var products_df=products_df1.toDF(columnsRenamed1:_*)    
//var second_rename=products_df.select("a","b","c","f").drop("d","e")



//CREATING ORDERS SCHEMA

  var first_df1=spark.read.csv("../dg_raju/orders_data.txt")
   //   first_df1.take(10).foreach(println)
      val columnsRenamed = Seq("order_id", "order_date", "order_customer_id", "order_status")
      val orders_DF = first_df1.toDF(columnsRenamed: _*)
 //   orders_DF.take(5).foreach(println)
orders_DF.createOrReplaceTempView("orders")
//spark.sqlContext.sql("select * from orders limit 10").show()
  //CREATING ORDER ITEM SCHEMA
 

  var first_df2=spark.read.csv("../dg_raju/order_item.txt")
   //   first_df2.take(10).foreach(println)
     val columnsRenamed2 = Seq("order_item_id", "order_item_order_id", "order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")
    
     val orders_DF_name = first_df2.toDF(columnsRenamed2: _*)
    
    var order_items_DF=orders_DF_name.select("order_item_id", "order_item_order_id", "order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")

//     order_items_DF.take(5).foreach(println)
order_items_DF.createOrReplaceTempView("order_items")
//spark.sqlContext.sql("select * from orders_items limit 10").show()
     

  
      var products_df1=spark.read.csv("../dg_raju/products/main_products")
     val columnsRenamed3 = Seq("product_id", "product_category_id", "product_name", "product_description","product_price","product_image")
     val product_DF_name = products_df1.toDF(columnsRenamed3: _*)
    
      var product_DF=product_DF_name.select("product_id", "product_category_id", "product_name", "product_description","product_price","product_image").drop("product_description","product_image")
     
     product_DF.createOrReplaceTempView("products")
//    spark.sqlContext.sql("select * from products limit 10").show()








val daily_revenue_per_product=spark.sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product " +
"FROM orders o JOIN order_items oi " +
"ON o.order_id = oi.order_item_order_id " +
"JOIN products p ON p.product_id = oi.order_item_product_id " +
"WHERE o.order_status IN ('COMPLETE', 'CLOSED') " +
"GROUP BY o.order_date, p.product_name " +
"ORDER BY o.order_date, daily_revenue_per_product desc")

import org.apache.spark.sql.functions.{col, lit, when}

var new_col_add=daily_revenue_per_product.withColumn("Killer_column",lit(0))
//var filter_revenue=dob.filter(dob["daily_revenue_per_product"]<"3000")
new_col_add.take(50).foreach(println)

//var filter_rdd=daily_revenue_per_product.filter(daily_revenue_per_product("order_date") === "2013-07-25 00:00:00.0").count
//println(filter_rdd)

//products_df.map(func, encoder)
  }
}