package cargaDatos

import org.apache.spark.{ SparkContext, SparkConf }
import java.util.Calendar
import java.text.SimpleDateFormat 
import org.apache.spark.rdd._

object GenerarBannerHDFS extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
  val sc = new SparkContext(conf)

  val rango0m = "0-10m"
  val rango1m = "10-20m"
  val rango2m = "20-30m"
  val rango3m = "30-40m"
  val rango4m = "40-60m"
  val rango5m = "60-80m"
  val rango6m = "80-100m"

  val rango0f = "0-10f"
  val rango1f = "10-20f"
  val rango2f = "20-30f"
  val rango3f = "30-40f"
  val rango4f = "40-60f"
  val rango5f = "60-80f"
  val rango6f = "80-100f"

  val banner0m = "banner rango 0-10 para ninos"
  val banner1m = "banner rango 10-20 para chicos"
  val banner2m = "banner rango 20-30 para hombres"
  val banner3m = "banner rango 30-40 para hombres"
  val banner4m = "banner rango 40-60 para hombres"
  val banner5m = "banner rango 60-80 para abuelos"
  val banner6m = "banner rango 80-100 para bisabuelos"

  val banner0f = "banner rango 0-10 para ninas"
  val banner1f = "banner rango 10-20 para chicas"
  val banner2f = "banner rango 20-30 para mujeres"
  val banner3f = "banner rango 30-40 para mujeres"
  val banner4f = "banner rango 40-60 para mujeres"
  val banner5f = "banner rango 60-80 para mujeres"
  val banner6f = "banner rango 80-100 para bisabuelas"

  val rdd:RDD[String] = sc.parallelize {
    Seq(
      s"$rango0m, $banner0m",
      s"$rango1m, $banner1m",
      s"$rango2m, $banner2m",
      s"$rango3m, $banner3m",
      s"$rango4m, $banner4m",
      s"$rango5m, $banner5m",
      s"$rango6m, $banner6m",
      s"$rango0f, $banner0f",
      s"$rango1f, $banner1f",
      s"$rango2f, $banner2f",
      s"$rango3f, $banner3f",
      s"$rango4f, $banner4f",
      s"$rango5f, $banner5f",
      s"$rango6f, $banner6f"
      )
  }
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)
  
  val ruta = s"hdfs://quickstart.cloudera:8020/user/cloudera/recomendador/$cadNow/banners"
  rdd.coalesce(1).saveAsTextFile(ruta)
  

}