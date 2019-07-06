package topgearclass
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
import org.apache.spark.sql.functions.explode
object XML_Parsing {
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
      
     var read=spark.read.format("xml").option("rowTag", "book").load("../books.xml")
     read.printSchema()

     read.createOrReplaceTempView("books")
     
     spark.sql("describe books").show()
     //spark.sql("select sum(price),genre  from books group by genre").show()
     //spark.sql("select *   from books limit 10").show()
    // spark.sql("select *  from books where genre='Computer' limit 10").show()
     spark.sql("select to_date(publish_date)   from books where month(publish_date)='12' limit 10").show()
    
     
     import org.apache.spark.sql.functions.lit
     
     var ans= convertDate(read)
   var ab= ans.withColumn("Ranghar", lit("WOWW"))
    ab.select("price","genre","publish_date","Ranghar")
    
     ab.take(5).foreach(println)
  }
       import org.apache.spark.sql.functions._
       
       def convertDate(df:DataFrame)=
       {
       var toDate=udf(
           (entry:String)=>
             {
               var returnval:java.sql.Date=null
               var simdatfor=new java.text.SimpleDateFormat("dd-MM-YYYY")
               
               try
               {
                 returnval=new java.sql.Date(simdatfor.parse(entry).getTime)
               }
               catch
               {
                 case e:Exception=>{
                 returnval=null
                     
                   }
               }
               
             
       returnval
             })
       df.withColumn("pub_date",toDate(df.col("publish_date"))).select("price","genre","publish_date")
       
     //     read.take(2).foreach(println)

     //
    // read.collect().foreach(println)
     /*  
      var df=sc.wholeTextFiles("../Food.xml")
      df.take(5).foreach(println)
      var df2=sc.textFile("../Food.xml")
      df2.take(5).foreach(println)
      
    */  
    
//    var load=sc.textFile("../Food.xml")
  //  load.take(5).foreach(println)
  
}
}