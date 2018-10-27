package learning
import org.apache.spark.SparkContext
object Ranking2 {
 
  
def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): Iterable[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

  val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
  val minOfTopNPrices = topNPrices.min

  val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

  topNPricedProducts
}
  
   
  
  
  
  
  def main(args:Array[String])
  {
   
    var sc=new SparkContext("local[*]","Ranking DataSet")
    
    var main_rdd=sc.textFile("../dg_raju/products/main_products")
  
    //Map the main value which we would make as key and rest product field to be converted 
    var productsMap=main_rdd.filter(product => product.split(",")(4) != "").
    map(product => (product.split(",")(1).toInt, product))
    
    val Products_Group_By=productsMap.groupByKey()//.take(10)//.foreach(println)
    
    //MY WAY OF GETTING TOP 5 products prices
    
    var top_5=main_rdd.filter(product => product.split(",")(4) != "").
    map(product => (product.split(",")(4).toFloat,""))//, product))
      
    var top_5_sort=top_5.distinct().sortByKey(false)
    top_5_sort.take(5).foreach(println);
    
    //DG_RAJU WAY top 5 prices
    
    var products_Iterable=Products_Group_By.first()._2   //We will see only for first partition that is 34 as this is first partition.
   
    //We will see only for first partition that is 34 as this is first partition.
    
    println("The size of of Iterable is "+products_Iterable.size)
    
    var map_product_iterable_array=products_Iterable.map(x=>x.split(",")(4).toFloat).toSet
    
    val topNPrices = map_product_iterable_array.toList.sortBy(p => -p).take(5)
    topNPrices.foreach(println)
    
    println("THE TOP N PRICES FOR THE CAtegory 34 is above") 
  
    
    
    //TOP 5 products and pricing 
    
    
    var top_5_products=products_Iterable.toList.sortBy(product => -product.split(",")(4).toFloat)
    println("\n\n\n")
    top_5_products.foreach(println)
    
    
    
    //GETTING THE minimum price 
    
    var minimum_product_price=topNPrices.min;
    println("The minimum price is "+minimum_product_price)
    
    
// DIFFERENCE BETWEEN TAKE WHILE and FILTER filter will iterate through whole input iterator while
//Take While will break once the predicate turn False,
//if you have an iterator with 1st element that false to predicate, takewhile will break at 1st iteration and return empty
    
    

  val topNPricedProducts = top_5_products.takeWhile(product => product.split(",")(4).toFloat >= minimum_product_price)
  
    

    
    
    
    
    
    
    
    
    
    
    
    
 /*   var sort_array=map_product_iterable_array.sorted	
    
    for(i<-sort_array.length-1 to 0)
    {    
      println(sort_array(i))
    }
   */ 
    
    
    
    
    
  }
}