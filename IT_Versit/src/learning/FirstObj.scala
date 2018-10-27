package learning

  object FirstObj {
  
  def first(func:Int=>Int,b:Int,c:Int):Int=
  {
    var total=0;
    for (i<-b to c)
    {
      total=i+func(i)
    }
  total
  }
  
  def square(x:Int)={x*x}
  
  def cube(x:Int)={x*x*x}
  
  
  
  
  def main(args:Array[String])
  {
  var a= (first(cube,1,10))
print(a)
  }

}