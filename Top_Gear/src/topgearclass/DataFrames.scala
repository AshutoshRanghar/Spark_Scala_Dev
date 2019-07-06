package topgearclass
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
object DataFrames {
  def main(args:Array[String])
  {
val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
  
      /*Spark >= 2.2
As of Spark 2.2 (released quite recently and highly recommended to use), you should use multiLine option instead. multiLine option was added in SPARK-20980 Rename the option wholeFile to multiLine for JSON and CSV.

scala> spark.version
res0: String = 2.2.0

scala> spark.read.option("multiLine", true).json("employee.json").printSchema
root*/
      
      
      var sc=new SparkContext("local[*]","Extract Date")
var df=spark.read.option("multiLine", true).json("../dg_raju")

df.collect().take(2).foreach(println)

//Extract Date




}
}