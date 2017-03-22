package als

import org.apache.log4j.{ Level, Logger }
import java.util.{Date, Properties}
import kafka.serializer.StringDecoder
import org.apache.spark.ml.recommendation.{ ALSModel, ALS }
import org.apache.spark.ml.param.Params
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
import org.apache.hadoop.hbase.spark._
import scala.collection.mutable.ListBuffer
import org.apache.kafka.clients.producer._

case class Fila(val userid: Int, val artidnum: Int)

object HacerRecomendacion extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Recomendador Spark")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(5))
  val sqlContext = new SQLContext(sc)
  //val ssc = new StreamingContext(sc, Seconds(15))
  val hostName = "sandbox.hortonworks.com"
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)
  
  val rutaLocal = s"/root/Documents/Parte1_Recomendador/model/$cadNow"

  val modelLoaded: ALSModel = ALSModel.load(rutaLocal)

  // consultar numero de artistas de la tabla de Hbase
  val hbaseConf = HBaseConfiguration.create()
  //esto puede cambiar dependiendo de hostname
  hbaseConf.set("hbase.zookeeper.quorum", hostName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
  //esto puede cambiar dependiendo de la propiedad de zookeeper
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");

  val hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  val tableName = TableName.valueOf("nsRec:artistWithName")
  val table: Table = hbaseConn.getTable(tableName);

  val scanner = table.getScanner(new Scan);
  var aux = 0

  while (scanner.next() != null) {
    aux = aux + 1
  }
  val numArtist = aux
  scanner.close()
  
  val brokers = "sandbox.hortonworks.com:6667"
  
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ProducerRec")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val topic = "topic-recomendaciones"
  val producer = new KafkaProducer[String, String](props);

  val topicSet: Set[String] = List("flume-channel").toSet

  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> brokers,
    "enable.auto.commit" -> "true",
    "group.id" -> "GroupRecomendar")

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

  kafkaStream.foreachRDD { rdd =>
    {
      rdd.foreachPartition { iter =>
        iter.foreach {
          case (key, msg) => {
            val cadEvento = msg
            val fields = cadEvento.split('\t')

            val iniclave = fields(0)

            val userid = iniclave.substring(iniclave.length - 5, iniclave.length())

            val recomendacion = crearRecomendacion(userid, table, numArtist)
            if (!recomendacion.isEmpty){             
             
             val bannerMos = recomendacion.get;             
             val bannerToKaf = new ProducerRecord[String, String](topic, key, bannerMos);
             producer.send(bannerToKaf)
            }          

          }
        }
      }
    }
  }

  //- comenzar streaming
  try {
    ssc.start()
    ssc.awaitTermination()

  } finally {
    sc.stop()
  }

  def crearRecomendacion(userid: String, table: Table, numArtist: Int): Option[String] = {

    var auxArt = new ListBuffer[Fila]()
    for (i <- 1 to numArtist) {
      // numero de user, numero de artista
      val cliente1 = Fila(1, i)

      auxArt += cliente1

    }

    val todosArtitas = auxArt.toSeq

    val todos: RDD[Fila] = sc.parallelize(todosArtitas)

    val todosDF = sqlContext.createDataFrame(todos)

    val predictions: DataFrame = modelLoaded.transform(todosDF).select("userid", "artidnum", "prediction")

    val tupla: RDD[(Int, Int, Float)] = predictions.rdd.map {
      x =>{
          val userid = x.getInt(0)
          val artidnum = x.getInt(1)
          val prediction = x.getFloat(2)
          (userid, artidnum, prediction)
        }
    }
    val ordenada = tupla.sortBy(x => x._3, false)

    val mayor = ordenada.take(1)(0)

    val artNum = mayor._2

    val get1 = new Get(Bytes.toBytes(artNum.toString()))

    if (table.exists(get1)) {
      val filaUser = table.get(get1);
      val qualArt = filaUser.getValue("datos".getBytes, "artname".getBytes());
      var artName = String.valueOf(Bytes.toString(qualArt))
      return Some(s"Usuario: $userid, te recomiendo el artista: $artName")
    } else {
      return None
    }

  }

}