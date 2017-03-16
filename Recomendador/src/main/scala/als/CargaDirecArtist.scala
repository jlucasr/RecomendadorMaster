package als

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }



object CargaDirecArtist extends App{
  
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark con Hive")
  System.setProperty("hive.metastore.uris", "thrift://sandbox.hortonworks.com:9083");
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)

  val hostName = "sandbox.hortonworks.com" 

  try {
    // consulta Hive para obtener artid diferentes:
    val ratingsDF: DataFrame = hiveContext.sql("SELECT artidnum, artid, artname FROM practica.artidwithnum")
    
    // lo transformo en un RDD de texto
    val cadArtis:RDD[String] = ratingsDF.map { x =>  
       new String(s"${x.get(0).toString()}\t${x.get(1).toString()}\t${x.get(2).toString()}")}
    
    
    // guardar la consulta en directorio hdfs
    val ruta = s"hdfs://$hostName:8020/user/cloudera/mrec/artIdNum"
    cadArtis.saveAsTextFile(ruta)
    
    // crear tabla hive que referencia ese directorio
    
  } finally {
    sc.stop()
  }
  
}