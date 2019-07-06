package databricks_learn

import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.col
object Json_Parsing {
  
  def main(args:Array[String])
  {
    var spark=SparkSession
    .builder().appName("Web_Logs_Scrapping")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
     
    var json1=spark.read.json("C:/Users/ashut/Desktop/Files_Spark/web_logs_json3.txt") 
      
    //    var json1=spark.read.json(spark.sparkContext.wholeTextFiles("C:/Users/ashut/Desktop/Files_Spark/web_logs_json.txt").values  )
  //var json1=spark.read.json("C:/Users/ashut/Desktop/Files_Spark/web_logs_json.txt", multiLine=True)
    
   // var df2=json1.withColumn("servlet-name", col("servlet-name")).select("servlet-name")
    //df2.show()
     json1.printSchema()
     json1.show()  
     
  }
}