package twitter_tweets

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import java.util.regex.Pattern
import java.util.regex.Matcher

object Log_Parsing_Ash {
  
/** Maintains top URL's visited over a 5 minute window, from a stream
 *  of Apache access logs on port 9999.
 */
  def main(args:Array[String])
  {
    
    val ssc=new StreamingContext("local[*]","Log Parser",Seconds(1))
   val pattern=apacheLogPattern()
   
   
    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
 //   val request=lines.map(x=>{val matcher:Matcher=>pattern.matcher(x)})
val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    var get1=requests.map(x=>{var rate=x.toString().split(" ");if (rate.size!=3) rate(1) else "ERROR" })
    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = get1.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
    
    
    
  
  
}