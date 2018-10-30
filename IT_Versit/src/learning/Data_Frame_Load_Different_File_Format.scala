package learning
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object Data_Frame_Load_Different_File_Format {
  def main(args:Array[String])
  {
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

      var orderDF=spark.read.json("../orders_json")
   //  orderDF.write.csv("../orders_csv")
    //  orderDF.write.orc("../orders_orc")
      
      var read_orc=spark.read.orc("../orders_orc")
      read_orc.collect().foreach(println)
  }
}