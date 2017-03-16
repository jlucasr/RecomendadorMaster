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
import org.apache.spark.streaming._

case class Fila(userid: Int, artidnum: Int)

object HacerRecomendacion extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Recomendador Spark")
  System.setProperty("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val sqlContext = new SQLContext(sc)
  val ssc = new StreamingContext(sc, Seconds(15))
  val hostName = "sandbox.hortonworks.com"
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)

  val modelLoaded: ALSModel = ALSModel.load("/root/Documents/Parte1_Recomendador/model")
    
  val cliente1 = Fila(1, 1)
  val cliente2 = Fila(1, 2)
  val cliente3 = Fila(1, 3)
  val todos: RDD[Fila] = sc.parallelize(Seq(cliente1,cliente2,cliente3))
  
  val todosDF = sqlContext.createDataFrame(todos)
  //val artistNames: DataFrame = hiveContext.sql("SELECT artid,artname,artidnum FROM practica.artidwithnum")
  
  val predictions: DataFrame  = modelLoaded.transform(todosDF).select("userid", "artidnum","prediction")
  
  //predictions.registerTempTable("predic")
   predictions.show
  
   //- comenzar streaming
//  try{
//     ssc.start()
//     ssc.awaitTermination()
//    
//  }finally{
//    sc.stop()
//  }
 
  
}