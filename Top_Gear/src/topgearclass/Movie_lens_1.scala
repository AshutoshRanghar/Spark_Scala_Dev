package topgearclass
import scala.io.Source

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object Movie_lens_1 {
  
  case class U_data(usersId:Int,movieId:Int,rating:Int,unix_time:String)
  case class Movie(movie_id:Int,movie_name:String,Genre:String)
  case class Users(UserId:Int,age:Int,gender:String,occupation:String,zip_code:String)
  case class Genre(types:String,type_Id:Int)
  def mapper_u_item(line:String):U_data={
  
    var fields=line.split("\t");
    
    val u_item:U_data=U_data(fields(0).toInt,fields(1).toInt,fields(2).toInt,fields(3).toString())
    
    return u_item
    
  }


  def mapper_Movie(line:String):Movie={
    var fields=line.split("|");
   
    val movie:Movie=Movie(fields(0).toInt,fields(1).toString(),fields(2).toString())
     return movie;  
  }
  
  def map_users(line:String):Users={
    var fields=line.split('|');
    
    val ret_user:Users=Users(fields(0).toInt,fields(1).toInt,fields(2).toString(),fields(3).toString(),fields(4).toString())
    
    return ret_user;
    
    
  }
  
  def map_genre(line:String):Genre={  
    var fields=line.split('|');
    var fields_gen:Genre=Genre(fields(0).toString(),(fields(1).trim()).toInt)
    
    return fields_gen
    }
  
  def main(args:Array[String])
  {
  
    val spark=SparkSession
     .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
  var lines_rat=spark.sparkContext.textFile("../ml-100k/u.data")
  var lines_movie=spark.sparkContext.textFile("../ml-100k/u.item")
  var lines_user=spark.sparkContext.textFile("../ml-100k/u.user")
  var lines_genre=spark.sparkContext.textFile("../ml-100k/u.genre")
  
 var rating=lines_rat.map(mapper_u_item)
 var movies=lines_movie.map(mapper_Movie)
 var users=lines_user.map(map_users)
 var genre=lines_genre.map(map_genre)
 import spark.implicits._
 var schema_rat=rating.toDS()
 
 var schema_movies=movies.toDS()
 var schema_users=users.toDS()
 var schema_genre=genre.toDS()
 
 schema_rat.createOrReplaceTempView("Ratings")
 //schema_movies.createOrReplaceTempView("Movies")
 schema_users.createOrReplaceTempView("Users")
schema_genre.createOrReplaceTempView("Genre")
 
  schema_rat.schema.fields.foreach(println)

  val query=spark.sql("select occupation,types,rating from Ratings inner join Users on Ratings.UsersId=Users.UserId  inner join Genre on Genre.type_id=Ratings.movieId limit 10")
  val query1=spark.sql("select * from Genre")
  
  val results=query.collect()
  results.foreach(println)
  spark.stop()
 

  /*
  case class U_data(usersId:Int,movieId:Int,rating:Int,unix_time:String)
  case class Movie(movie_id:Int,movie_name:String,Genre:String)
  case class Users(UserId:Int,age:Int,gender:String,occupation:String,zip_code:String)
  case class Genre(types:String,type_Id:Int)
   * 
   * 
   */
  
  
  
  
  
  
  
  
  
  }
}