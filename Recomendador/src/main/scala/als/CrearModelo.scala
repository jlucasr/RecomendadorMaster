package als

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.{ALSModel, ALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._ 

case class Rating(userId: Int, artId: String, rating: Double)

object CrearModelo extends App{
  
  // metastore 
  // thrift://sandbox.hortonworks.com:9083
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark con Hive")
  System.setProperty("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  val numIterations = 10
  val rank = 10
  
  try{
      val ratingsDF:DataFrame = hiveContext.sql("SELECT * FROM practica.ratingsperartist")
  
      val numIterations = 10
      val rank = 10
      
      ratingsDF.schema.printTreeString()
      //val als = new ALS().setUserCol("userid").setItemCol("artid").setRank(rank).setMaxIter(numIterations)

      //training the model
      //val model: ALSModel = als.fit(ratingsDF.toDF())
      
      
      
  }finally{
    sc.stop()
  }
  
}