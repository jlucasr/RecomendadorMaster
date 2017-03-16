package als

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.ml.recommendation.{ ALSModel, ALS }
import org.apache.spark.ml.param.Params
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.streaming._
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
import org.apache.hadoop.hbase.spark._
import scala.collection.mutable.ListBuffer

case class Fila(val userid: Int,val artidnum: Int)

object HacerRecomendacion extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Recomendador Spark")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //val ssc = new StreamingContext(sc, Seconds(15))
  val hostName = "sandbox.hortonworks.com"
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)

  val modelLoaded: ALSModel = ALSModel.load("/root/Documents/Parte1_Recomendador/model")
  
  // consultar numero de artistas de la tabla de Hbase
  val hbaseConf = HBaseConfiguration.create()
  //esto puede cambiar dependiendo de hostname
  hbaseConf.set("hbase.zookeeper.quorum", hostName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
  //esto puede cambiar dependiendo de la propiedad de zookeeper
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
  
  val hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  val tableName = TableName.valueOf("nsRec:artistWithName")
  val table:Table = hbaseConn.getTable(tableName);
  
  val scanner = table.getScanner(new Scan);
  var numArtist = 0
  
  while (scanner.next() != null){
    numArtist = numArtist + 1
  }
  scanner.close()
  println("Numero de artistas: "+numArtist)
  
  var auxArt = new ListBuffer[Fila]()
  for (i <- 1 to numArtist){
    // numero de user, numero de artista
    val cliente1 = Fila(1, i)
    
    auxArt += cliente1

  }
  val todosArtitas = auxArt.toSeq
  
  val todos: RDD[Fila] = sc.parallelize(todosArtitas)
  
  val todosDF = sqlContext.createDataFrame(todos)
  
  
  val predictions: DataFrame  = modelLoaded.transform(todosDF).select("userid", "artidnum","prediction")
  
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