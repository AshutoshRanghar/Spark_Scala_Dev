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
object Custom_Schema_Spark {
  
  def main(args:Array[String])
  {
    
    val csvSchema = StructType(
  List(
    StructField("Invoice", StringType, false),
    StructField("StockCode", StringType, false),
    StructField("Description", StringType, false),
     StructField("Quantity", IntegerType, false),
    StructField("InvoiceData", StringType, false),
    StructField("UnitPrice", DoubleType, false),
     StructField("Customer_Id", IntegerType, false),
    StructField("Country", StringType, false)
    
    
  )
)


val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
     import spark.implicits._
     
       var Df=spark.read
 .option("inferSchema", "true")
 
 .schema(csvSchema)
 .csv("../online_csv1.csv")
 //Drop the unused column 
 .drop("InvoiceData")
 
 Df.printSchema()
 Df.take(2).foreach(println) 
 
  var Df1=Df.coalesce(2)
   
  println("Partitions: " + Df1.rdd.getNumPartitions)
  
  //printRecordsPerPartition(csvDF)

  println("-"*80)  
  
  
  }   
}