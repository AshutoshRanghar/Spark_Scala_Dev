package twitter_tweets
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

import java.util.concurrent._
import java.util.concurrent.atomic._

object Most_Popular_Hashtag {
 
  def main(args:Array[String])
  {
   setupTwitter()
   val ssc=new StreamingContext("local[*]","Spark Streaming",Seconds(2))
   
   setupLogging()
   
   val tweets=TwitterUtils.createStream(ssc,None)
   val status=tweets.map(x=>x.getText)
  
  val tweetword=status.flatMap(x=>x.split(" "))
  
  
 val hastag=tweetword.filter(x=>x.contains("Meetoo"))
  
  //val map_hash=tweetword.map(x=>(x,1))
  
 // val reduce_map_hash=map_hash.reduceByKeyAndWindow( (x,y) =>x+y , (x,y)=>x-y, Seconds(300),Seconds(2))
    
  //var sorter=reduce_map_hash.transform(rdd=>rdd.sortBy(x=>x._2,false))
  
 //hastag.print()
 hastag.print()
   
 //sorter.print
  
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
