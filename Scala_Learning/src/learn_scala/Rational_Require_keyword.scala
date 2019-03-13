package learn_scala

  class Rational_Require_keyword(var n:Int,var m:Int) {
  
  require(n!=0,"No it should not be")
  
  override def toString:String=s"R(n/m)"
  
  def check()
  {
    var m:Int=50
    
    //Instance Values
    println(this.m+" "+this.n+" ")
 
    //Local Values 
    
    println(m+" "+n)
    
  }  
  
  def min(other:Rational_Require_keyword):Rational_Require_keyword=
  {
 var m:Int=5000
    
 println(m+" "+n)
 println(this.m+" "+this.n)
 
 
 return this
    }

  


  
  }


  object Major
  {
  def main(args:Array[String])
  {
    var a=new Rational_Require_keyword(100,12)
     a.m=4000
   
     
     a.check()
    a.min(a)
    
    var b=new Rational_Require_keyword(1,2)
    var c=new Rational_Require_keyword(3,4)
  
    
    
    }
  }