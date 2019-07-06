
package topgearclass


trait Animal
{
  def bark()
}

class Traits_Learn(line:String) extends Animal {
  
  def bark()
  {
    
    println("BHOAW BHOAW")
    println(line)
  
  }
  
}

 object Traits
 {
  def main(args:Array[String])
  {
  
    var ob=new Traits_Learn("Default_Constructor")
    ob.bark()
    
  }
}