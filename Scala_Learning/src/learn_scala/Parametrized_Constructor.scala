package learn_scala

class Parametrized_Constructor(var a:Int,val b:Int) {
 
  def display_constructor()
  {
    
    println(a)
    println(b)
  }
  
  
  
}


object Calling_Main
{
  def main(args:Array[String])
  {
    
  
  val ob=new Parametrized_Constructor(10,20)
  //Chaning Parameterized Constructor Values
  ob.a=10000;
  
  ob.display_constructor()
  
  val ob2=new Parametrized_Constructor(10,20)
  ob2.display_constructor()
  }
}
  
  
  
  
  
  
