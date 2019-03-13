package learn_scala

class Basic_Class(var a1:Int,b1:Int) {
 val x:Int=0
 val y:Int=10;
 
  def add(b:Int,c:Int)={
  var g=b*c
  println(g)
  
  println(a1+""+b1)
  }


}  
  object Main_basic 
  {
    def main(args:Array[String])
    {
  
    val ab=new Basic_Class(10,11)
   ab.a1=20;
    ab.add(5, 10)  
   

    }
  }

  
