package topgearclass
import com.databricks.spark.xml
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.explode

import org.apache.spark.sql.functions.col
object XML_PARSING_1 {
  def main(args:Array[String])
  {
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
     import spark.implicits._
       var df=spark.read.format("xml").option("rowTag", "book").load("../books.xml")
      df.printSchema()
       
      
      df.show()
      df.select(col("_id"),explode(col("batters.batter"))).show()
      
      
      
    //   var  bdf=df.withColumn("Batterss", explode($"batters.batter"))
      
     //  var new_df=bdf.map(x=>(x.getString(0),x.getString(1),x.getString(2)))
      // var new_df=bdf.select("batters").toDF().select(col("batters"))
      
     //new_df.show()
      
      
      //bdf.show()
          
      
       //new_df.show() 
      
     // new_df.collect().foreach(println)
      
        
      
      
      // bdf.printSchema()
     // bdf.show()
      //print(new_df)
      //new_df.show()
  }
}