package als

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.ml.recommendation.{ ALSModel, ALS }
import org.apache.spark.ml.param.Params
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import java.util.Calendar
import java.text.SimpleDateFormat

case class Row(userid: Int, artidnum: Int, rating: Double)

object HacerRecomendacion extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Recomendador Spark")
  System.setProperty("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val hostName = "sandbox.hortonworks.com"
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)

  val modelLoaded: ALSModel = ALSModel.load("/root/Documents/Parte1_Recomendador/model")
    
  val cliente1 = Row(1, 1,1.0)
  val cliente2 = Row(1, 2,1.0)
  val cliente3 = Row(1, 3,1.0)
  val todos: RDD[Row] = sc.parallelize(Seq(cliente1,cliente2,cliente3))
  val sqlContext = new SQLContext(sc)
  val todosDF = sqlContext.createDataFrame(todos)
  val artistNames: DataFrame = hiveContext.sql("SELECT artid,artname,artidnum FROM practica.artidwithnum")
  
  val predictions = modelLoaded.transform(todosDF).select("userid", "artidnum", "rating","prediction")
  predictions.registerTempTable("predic")
  artistNames.registerTempTable("artist")

  val predictionsWithArtist = sqlContext.sql("SELECT * FROM artist A JOIN predic B ON (A.artidnum = B.artidnum )").show
  //val predictionsWithArtist = predictions.join(artistNames, predictions("artidnum")  === artistNames("artidnum"),"inner").show
  
  
  
}