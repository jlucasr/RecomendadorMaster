package streaming

import java.util.{ Date, Properties }
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.hadoop.hbase.spark._
import org.apache.kafka.clients.producer._

import org.apache.spark.rdd._

object RecibidorEventos extends App with BusquedasEventos {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("My App")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Milliseconds(300))

  val topicSet: Set[String] = List("flume-channel").toSet

  val brokers = "sandbox.hortonworks.com:6667"

  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> brokers,
    "enable.auto.commit" -> "false",
    "group.id" -> "GroupEventos")

  val hbaseConf = HBaseConfiguration.create()
  //esto puede cambiar dependiendo de hostname
  val hostName = "sandbox.hortonworks.com"
  hbaseConf.set("hbase.zookeeper.quorum", hostName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
  //esto puede cambiar dependiendo de la propiedad de zookeeper
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");

  val hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  val hbaseContext = new HBaseContext(sc, hbaseConf);
  val admin = hbaseConn.getAdmin()

  val tableEventName = TableName.valueOf("nsRec:eventosCliente")
  val tableCliName = TableName.valueOf("nsRec:clientes")
  val tabledescriptor = new HTableDescriptor(tableEventName);
  val nameFam = "datos".getBytes()
  val familyDatos = new HColumnDescriptor(nameFam);
  tabledescriptor.addFamily(familyDatos);
  familyDatos.setMaxVersions(1)
  if (!admin.tableExists(tableEventName)) {
    admin.createTable(tabledescriptor);
  }
  //obtener las dos tablas: eventos y clientes
  val tableEvent = hbaseConn.getTable(tableEventName)
  val tableClients = hbaseConn.getTable(tableCliName)
  val tableBanners = hbaseConn.getTable(TableName.valueOf("nsRec:banners"));

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

  // Productor de mensajes a kafka
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val topic = "TopicBanners"
  val producer = new KafkaProducer[String, String](props);

  // por cada evento del kafka stream:

  kafkaStream.foreachRDD { rdd =>
    {
      rdd.foreachPartition { iter =>
        iter.foreach {

          case (key, msg) => {
            val cadEvento = msg
            val fields = cadEvento.split('\t')

            val fieldSize = fields(0).size
            val clave = String.valueOf((fields(0).substring(fieldSize - 6, fieldSize).toInt))
            //compruebo si hay cliente insertado con esa clave
            val get1 = new Get(Bytes.toBytes(clave))
            val put = new Put(Bytes.toBytes(clave))

            if (tableEvent.exists(get1)) {
              val filaUser = tableEvent.get(get1);
              val qualRep = filaUser.getValue(nameFam, "numReprod".getBytes());
              // sumo 1 al num de reproducciones
              val numRep: Integer = Integer.parseInt(String.valueOf(Bytes.toString(qualRep)))

              val nuevoNum: Integer = numRep + 1

              if (nuevoNum >= NUM_EVENTOS_BANNER) {
                // get para obtener el valor de banner:
                val get4 = new Get(Bytes.toBytes(clave))
                val filaEvents = tableEvent.get(get4);
                val bannerQual = filaUser.getValue(nameFam, "banner".getBytes())
                val bannerMos = String.valueOf(Bytes.toString(bannerQual));

                val bannerToKaf = new ProducerRecord[String, String](topic, clave, bannerMos);
                producer.send(bannerToKaf)

                // como se envia el banner se pone el contador a 0
                put.addColumn(nameFam, "numReprod".getBytes(), "0".getBytes())
              } else {
                put.addColumn(nameFam, "numReprod".getBytes(), String.valueOf(nuevoNum).getBytes())
              }

            } else {
              // creo una nueva fila en la tabla, asignado el banner
              // que corresponde a ese usuario
              put.addColumn(nameFam, "numReprod".getBytes(), "1".getBytes())

              // get a la tabla de usuarios para saber sexo y edad para asignar el banner
              val get2 = new Get(Bytes.toBytes(clave))
              val filaUser = tableClients.get(get2);
              var hasEdad = false
              var hasSexo = false

              if (!filaUser.isEmpty()) {
                if (filaUser.containsColumn(nameFam, "edad".getBytes())) {

                  hasEdad = true
                }
                if (filaUser.containsColumn(nameFam, "sexo".getBytes())) {

                  hasSexo = true
                }
                if (hasEdad && hasSexo) {
                  //busco el banner en la tabla de banners

                  val recEd = filaUser.getValue(nameFam, "edad".getBytes());
                  val edad: Integer = Integer.parseInt(String.valueOf(Bytes.toString(recEd)))
                  val recSx = filaUser.getValue(nameFam, "sexo".getBytes());
                  val sexo: String = String.valueOf(Bytes.toString(recSx))

                  val cadBuscar = getBanner(edad, sexo)
                  if (!cadBuscar.isEmpty) {
                    // hago un get de la tabla de banners con la cadena anterior
                    val get3 = new Get(cadBuscar.get.getBytes);
                    val filaBanner = tableBanners.get(get3);
                    val qualBanner = filaBanner.getValue("banner".getBytes(), "cadBanner".getBytes());

                    //inserto el banner en tabla de evntos con nuevo qualifier

                    put.addColumn("datos".getBytes(), "banner".getBytes(), qualBanner)

                  }

                } else {
                  // inserto banner generico
                  val cadByte = "banner generico".getBytes()
                  put.addColumn("datos".getBytes(), "banner".getBytes(), cadByte)
                }

              }else{
                println("No se ha encontrado el cliente")
              }

            }
            tableEvent.put(put)
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
    producer.close()
  }

}