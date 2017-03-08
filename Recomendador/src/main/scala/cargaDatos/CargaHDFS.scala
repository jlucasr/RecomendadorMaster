package cargaDatos

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer

object CargaHDFS {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("My App")

    val ssc = new StreamingContext(conf, Seconds(15))

    val topicSet: Set[String] = List("TopicSongs").toSet

    val kafkaParams = Map[String, String]("bootstrap.servers" -> "sandbox.hortonworks.com:6667","enable.auto.commit"->"true")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    // Get the lines, print them
    println("Contenido del topic Kafka ")
    val lines = kafkaStream.map(_._2)
    
    // guardamos en hdfs
    lines.saveAsTextFiles("/root/Documents/pruebasSalido/eventos")
    

    ssc.start()
    ssc.awaitTermination()

  }

}