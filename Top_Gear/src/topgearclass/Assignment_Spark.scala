package topgearclass
import org.apache.spark.SparkContext


 object Assignment_Spark {
  
  def key_val(line:String)=
   
  {
  
    var fields=line.split(",")
    (fields(0).toString(),fields(6).toInt)
  
  }
  
  def total_day(line:String)=
  {
    var fields=line.split(",")
    (fields(0).toString(),fields(8),fields(20))
  
  }
  
  def international_met(line:String)=
  {
    
    var fields=line.split(",")
    (fields(0).toString(),fields(4).toString())
    
    
  }
  
  def both_plan(line:String)=
  {
    var fields=line.split(",")
    (fields(0).toString(),fields(4).toString(),fields(4).toString())
    
  }
  
  
 def checkInternationalPlan(givenParam:String):Boolean={
    
      val regex_pattern="[0-9]*".r
      givenParam match{
      
      case regex_pattern()=>true
     
      case "no"=>true
      
      case _ =>false
    
      }
  }
   
  
  def main(args:Array[String])
  {
    
    var sc=new SparkContext("local[*]","Assigment")
    
    var main_rdd_temp=sc.textFile("../Interview_questions/customer-chur.csv")
    
    var removal=(main_rdd_temp.take(1)(0)).toString()
   
    var main_rdd =main_rdd_temp.filter(x=>x!=removal)
    
    //Distinct States
    
    var dis_states=main_rdd.map(x=>x.split(",")(0))
    
    var distinct_states=dis_states.distinct()
    
    var collect1=distinct_states.collect()
    
    collect1.foreach(println)
    
    
    // Character_states
    
    var two_char=distinct_states.filter(x=>x.toString().length()==2)
    
    var collect2=two_char.collect()
    
    collect2.foreach(println)
    // Also true false thing
    
    var two_char1=distinct_states.map(x=>x.length()==2)
    
    var collect21=two_char1.collect()
    
    collect21.foreach(println)
    
    //Maximum number of Vmail message
    
    var vmail=main_rdd.map(x=>x.split(",")(6).toInt)
    
    var max=vmail.max()
    
    println("The max vamils iis ",max) 
    
    //Maximum number of message of OH,OH,RI States
    
    var max_message_rdd=main_rdd.map(key_val)
    
    var mapper=max_message_rdd.filter(x=>x._1=="OH"||x._1=="OK"||x._1=="RI")  
    
    var mapper1=mapper.reduceByKey(math.max(_,_))
    
    var collect3=mapper1.collect()
    
    collect3.foreach(println)
    
    //Average of total night minutes
    
    var night_min=main_rdd.map(x=>x.split(",")(13).toDouble)
    
    var avg=night_min.reduce((x,y)=>(x+y));
    
    println("SUM iS ",avg)
    
     //SUM oF DAY CALL FOR CUSTOMER WHO HAVE NOT CHURNED AN BELONG To OH AND NY
    
    var mapper2=main_rdd.map(total_day)
    
    var not_churned=mapper2.filter(x=>x._3=="False")
    
    
    var day_call=not_churned.filter(x=>x._1=="OH"||x._1=="NY")  
    
    var sample=mapper2.take(3);
   
    println("THIS iS ITHE A DD S")
    
    sample.foreach(println)
    
   // var day_call_reduce=day_call.reduceByKey((x,y)=>(x+y))
   
   var day_call_count=day_call.count()
   
   println("The day call cunt of the assignment is ",day_call_count)
    
   //day_call_reduce.foreach(println)
    
   ///INTERNATIONAL PLAN
   
    var int_inp=main_rdd.map(international_met)
    
    var international=int_inp.filter(x=> !(x._2 contains("no"))&& !(x._2 contains("yes")))
    
    var sample1=international.take(2)
   
    sample1.foreach(println)
   
   
    //  Both International and voice 
   
   var both=main_rdd.map(both_plan)
   
   var both_filter=both.filter(x=>(x._2=="yes") &&(x._3=="yes"))
   
   var take1=both_filter.take(3)
   
   take1.foreach(println)
   
   //Pattern Matching
   
   
  val check=mapper2.take(2)
   
  var regex="[0-9]*".r
   
  
  val map1=check.map(x=>regex.pattern.matcher(x._2).matches())
   
  var rdd=map1.take(1000)
  
  rdd.foreach(println)
  
  //SHORTCUT OF DOING THINGS 
  
  var last_rdd=main_rdd.filter(x=>x!=removal).map(x=>x.split(",")).filter(x=>checkInternationalPlan(x(8))).count()
 
  println(last_rdd)
  
  
    }
 
}