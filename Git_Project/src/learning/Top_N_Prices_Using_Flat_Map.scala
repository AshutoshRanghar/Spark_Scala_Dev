package learning

import org.apache.spark.SparkContext
object Top_N_Prices_Using_Flat_Map {
  
def getTopNPricedProducts(productsIterable: Iterable[String], topN: Int): List[String] = {
  val productPrices = productsIterable.map(p => p.split(",")(4).toFloat).toSet
  val topNPrices = productPrices.toList.sortBy(p => -p).take(topN)

val productsSorted = productsIterable.toList.sortBy(product => -product.split(",")(4).toFloat)
 val minOfTopNPrices = topNPrices.min

 val topNPricedProducts = productsSorted.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)

topNPricedProducts.foreach(println)
  topNPricedProducts
}

  
  
  def main(args:Array[String])
  {
    
    
    var sc=new SparkContext("local[*]","Ranking DataSet")
    
    var main_rdd=sc.textFile("../dg_raju/products/main_products")
  
    //Map the main value which we would make as key and rest product field to be converted 
    var productsMap=main_rdd.filter(product => product.split(",")(4) != "").
    map(product => (product.split(",")(1).toInt, product))
    
    
    /*THIS WILL HAVE PRODUCTS GROUP BY CATEGORY........................
    eg group by Owner name
  
    productsMap.groupByKey()//.foreach(println)
    
    
    Ash ( 100,200,300,400,500,600,600)
    Bash ( 100,200,300,400,500,600,700)
    Eash ( 100,200,300,400,500,600,700,800)
    Dash ( 100,200,300,400,500,600,700,800,900)
    
    
   var flat_map_products=Products_Group_By.flatMap(x=>getTopNPricedProducts(x._2, 2)) 
    
    when we are passing this function we are getting top N prices by category eg :
    
    Top 3 price so
    
    Ash -600,600,400 ----As 600 is two times it would be repeated
    Bash  700,600,500
    Eash  800,700,600
    Dash  900,800,700
    
    
    
    
    
    */
    
    
    val Products_Group_By=productsMap.groupByKey()//.foreach(println)
    
    
       //We will see only for first partition that is 34 as this is first partition.
    
    
    
    var mapper_check=Products_Group_By.map(x=>x._2)
   mapper_check.take(5).foreach(println)
    
   var topNPriceByCategory=Products_Group_By.flatMap(x=>getTopNPricedProducts(x._2, 2)) 
    
   topNPriceByCategory.foreach(println)
    

    

    
    
    
  }
}