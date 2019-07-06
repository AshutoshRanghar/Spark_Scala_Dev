package databricks_learn

import com.databricks.spark.xml
import org.apache.spark._
import org.apache.spark.sql._

import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
object First_Chapter_Loading {
  case class Online_Retail(Invoice:String,StockCode:String,Description:String,Quantity:Int,InvoiceData:String,UnitPrice:Double,
      Customer_Id:Integer,Country:String)
  
      
      
      def convertSchema(line:String):Online_Retail={
    val fields=line.split(",")
    var online=new Online_Retail(fields(0),fields(1),fields(2),fields(3).toInt,fields(4),fields(5).toDouble,fields(6).toInt,fields(7))
    
    return online
    
  }
      
      
  def main(args:Array[String])
  {
 //   var sc=new SparkContext("local[*]","XML Parsing")
       val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
     import spark.implicits._
 
//     var rdd1=spark.read.option("header","true")
 //.option("inferSchema", "true")
 //.csv("../Online Retail.csv")
var rdd1=spark.sparkContext.textFile("../Online Retail.csv")
         
 var Df1=rdd1.map(convertSchema).toDF()
 
 Df1.printSchema()
 Df1.take(5).foreach(println)

 

 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
     
        }
}















