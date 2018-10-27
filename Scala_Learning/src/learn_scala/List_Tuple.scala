package learn_scala
import scala.collection.mutable;
object List_Tuple {
  def main(args:Array[String])
{
    var l1= List(1,2,3,4)
    
    var l2=new Array[String](5)
    var l3=new Array[Int](5)
    
    l3=l1.toArray
    
    l3.foreach(args=>println(args))
 
    //Create a tuple
    
    var v1=new Tuple3(1,"Ash","Doon Blossoms")
    v1.productIterator.foreach{ i=>println(i)}
    
    
    var v3=mutable.Set(2,3,4)
    
    var v4=Set(1,2,3,4,6)
    /*Mutable means you can alter the collection in-place. So, if you have a collection c and you append an element with +=, then c has changed, and so has every other reference to that collection.

Immutable means that the collection object never changes; instead, you build new collection objects with operations such as + or ++, which return a new collection. This is useful in concurrent algorithms, since it requires no locking to add something to a collection. It may come at the cost of some overhead, but this property can be very useful. Scala's immutable collections are fully persistent data structures.

The difference is very similar to that between var and val, but mind you:

You can modify a mutable collection bound to a val in-place, though you can't reassign the val
you can't modify an immutable collection in-place, but if it's assigned to a var, you can reassign that var to a collection built from it by an operation such as +.
Not all collections necessarily exist in mutable and immutable variants; the last time I checked, only mutable priority queues were supported.
   
   * 
   * 
   */
     v4=v4+7
    print(v4)
    
    
    v3+=4;
    print(v3)


}
}