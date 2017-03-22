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
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.util.MLWriter

object CrearModelo extends App {

  // metastore 
  // thrift://sandbox.hortonworks.com:9083
  val hostName = "sandbox.hortonworks.com"
  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark con Hive")
  val sc = new SparkContext(conf)
  
  val fileConf = new Configuration()
  fileConf.set("fs.defaultFS", s"hdfs://$hostName:8020")
  val fs = FileSystem.get(fileConf)
  
  val output = fs.create(new Path("/user/cloudera/mrec/model/a1.txt"))

  System.setProperty("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
 
  val hiveContext = new HiveContext(sc)
  val numIterations = 10
  val rank = 10

  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)

  try {
    val ratingsDF: DataFrame = hiveContext.sql("SELECT * FROM practica.ratingsWithArtist")

    val numIterations = 10
    val rank = 10

    ratingsDF.schema.printTreeString()

    val rddRatings = ratingsDF.rdd

    val als = new ALS().setUserCol("userid").setItemCol("artidnum").setRatingCol("rating").setRank(rank).setMaxIter(numIterations)

    val model: ALSModel = als.fit(ratingsDF)

    val rutaLocal = s"/root/Documents/Parte1_Recomendador/model/$cadNow"
    
    model.write.overwrite().save(rutaLocal)

    
  } finally {
    sc.stop()
  }

}