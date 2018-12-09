package learn_scala

object Singleton_Class {

  private var m:Int=876

  var sc:Singleton_Class=new Singleton_Class
  
  println(sc.class_var)
}

class Singleton_Class
{
  import Singleton_Class._
  
  private var class_var:Int=200;

  
  def show()
  {
    println("Value is "+m)
  }
  
  
}

object hello
{
  def main(args:Array[String])
  {
    var ob:Singleton_Class=new  Singleton_Class
    ob.show()
  
  }
}