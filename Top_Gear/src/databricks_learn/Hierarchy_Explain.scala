package databricks_learn

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


case class Hierarchy(emp_id:String,emp_name:String,manager_id:String)
object Hierarchy_Explain {

  
   def assign(line:String):Hierarchy={
   
     var fields=line.split(",")
    
    var returner=new Hierarchy(fields(0),fields(1),fields(2))
    
    returner
 
   }
  
  def main(args:Array[String])
  {
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
     
    
     import spark.implicits._
     
     var Df1=spark.sparkContext.textFile("../hierarchy.csv")
       
     var DF2=Df1.map(assign)
       
     var Df=DF2.toDF()
         
     Df.createOrReplaceTempView("emp")
  
     Df.sqlContext.sql(
"With ash as (select emp_id,emp_name,manager_id from emp where emp_id=7 union all ( select * from emp join ash  on emp.emp_id=ash.manager_id) ) select * from ash " ).show()
       
       // Df.take(5).foreach(println)
       
  }
}
