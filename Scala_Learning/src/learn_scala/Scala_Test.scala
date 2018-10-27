package learn_scala

object Scala_Test {
  
  def main(args:Array[String])
  {
  var args=8
  var counter:Int=6
  print(args.getClass())
  //WHILE LOOP
  while (args>=5)
  {
    println("Check1")
 
    args=args-1
  }
  
  //FOR LOOP 
  for( i<- args to counter)
  { 
    println("HIVE")
  }
  
 // Arrays in Scala
  
  var mylist=Array(1,2,3,4)
  
  mylist.foreach(abc=>println("This is the other way"+abc))
  
  mylist.foreach(println)
}
}
