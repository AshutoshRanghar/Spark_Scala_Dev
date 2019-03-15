package twitter_tweets
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._
object WordCountTweet {
  
  def main(args:Array[String])
  {
  
  setupTwitter()
  val ssc=new StreamingContext("local[*]","Word_Count_Tweets",Seconds(1))
 ssc.checkpoint("../")
  
  var tweets =TwitterUtils.createStream(ssc,None)
   setupLogging()
  var status=tweets.map(x=>x.getText())
  
  var counter=status.count()
  
  
  var flat_map1=status.flatMap(x=>x.split(" "))
  
  var flat_map2=flat_map1.map(x=>(x,1))
  
  var agg=flat_map2.reduceByKeyAndWindow((x,y)=>(x+y), (x,y)=>(x-y), Seconds(1000), Seconds(1000))
  
  agg.print()
  
  
  flat_map2.print()
  
  
   // Kick it all off
    ssc.start()
    
    ssc.awaitTermination()
 
  
  }
  
}