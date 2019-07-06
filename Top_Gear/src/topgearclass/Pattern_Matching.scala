package topgearclass
import org.apache.spark.SparkContext
import javafx.animation.KeyValue

object Pattern_Matching {
  def checkIntPlan(param:String):Boolean={
   
    param match
   
   {
     case "yes" =>true
     case "no" =>true
     case _ =>false
     
   }
  }
 
  def check_num(str:String)
  {
    val Key_val_reg="[0-9]*".r
var ab=  Key_val_reg.findFirstMatchIn(str)
  match {
      case Some(_)=> println("Yes something is present")
     
      case None=>println("nothing")
      
    }

  for( i <- Key_val_reg.findAllMatchIn(str))
  {
    println(i.group(0))
  }
    
    }
    

  def log_file(str:String)
  {
    val Key_val_reg="(\\[.*\\])+".r
    
var ab=  Key_val_reg.findFirstMatchIn(str)
  match {
      case Some(_)=> println("Yes something is present")
     
      case None=>println("nothing")
      
    }

  for( i <- Key_val_reg.findAllMatchIn(str))
  {
    println(i.group(0))
  }
    
    }

  
  def url_file(str:String)
  {
    val Key_val_reg="(http.*)+".r
    
var ab=  Key_val_reg.findFirstMatchIn(str)
  match {
      case Some(_)=> println("Yes something is present")
     
      case None=>println("nothing")
      
    }

  for( i <- Key_val_reg.findAllMatchIn(str))
  {
    println(i.group(0))
  }
    
    }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
    


  
  def main(args:Array[String])
  {
  var sc=new SparkContext("local[*]","Spark_Context")
  
  var main_rdd_temp=sc.textFile("../customer-chur.csv")
 
   var removal=(main_rdd_temp.take(1)(0)).toString()
   
   var main_rdd =main_rdd_temp.filter(x=>x!=removal)
    main_rdd.take(2).foreach(println)

    //CHECK WHETHER ONLY TRUE AND FALSE IS PRESENT
    
    
  var check_condition=main_rdd.map(x=>x.split(",")).filter(x=>checkIntPlan(x(4))).count()
 
  println(check_condition)
  
  //ANOTHER WAY OF DOING SAME PROB
  
  var filter_use=main_rdd.map(x=>x.split(",")).filter(x=>x(4)=="yes"||x(4)=="no").count()
  
 println(filter_use)
  
 //Check the Pattern Testing
 
 var ab="ASHUTOSH J!@$!31"
 
 var test_string="asdsda[29-Nov-2015:04:57:55 +0000])dawdadwwd"  
 check_num(ab)
log_file(test_string) 
 
var url_test=" Googlebot/2.1; +http://www.google.com/bot.html)"
 url_file(url_test)
 
  
  }  
}