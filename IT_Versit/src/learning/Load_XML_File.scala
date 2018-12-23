package learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.databricks.spark.xml;
object Load_XML_File {
  def main(args:Array[String])
  {
     
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
      var df=spark.read.format("xml").option("rowTag", "food").load("../Food.xml")
  }
}