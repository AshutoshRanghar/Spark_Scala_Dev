package learn_scala

object Concatination_In_List {
  def main(args:Array[String])
{
    var a=List(1,2,3,4)
    var b=List(1,2,3,4)
    
    a=a:+4
    print(a)
}
}