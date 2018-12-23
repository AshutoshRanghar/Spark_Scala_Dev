package learning
import org.apache.spark.sql._
object Json_Loading {
  def main(args:Array[String])
  {
    
    var sc=SparkSession.builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      
      var df=sc.read.json("../dg_raju/orders_json")
      
     
    //  df.take(5).foreach(println)
      df.printSchema();
      df.createOrReplaceTempView("orders")
      
      sc.sql("select distinct order_date from orders").show();
    
  }
}