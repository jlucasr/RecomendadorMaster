package cargaDatos

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.hadoop.hbase.spark._
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.client.Result

object UpdateBannerHBase extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
  val sc = new SparkContext(conf)

  val hbaseConf = HBaseConfiguration.create()
  //esto puede cambiar dependiendo de hostname
  val hostName = "quickstart.cloudera"
  hbaseConf.set("hbase.zookeeper.quorum", hostName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
  //esto puede cambiar dependiendo de la propiedad de zookeeper
  hbaseConf.set("zookeeper.znode.parent", "/hbase");

  val hbaseConn = ConnectionFactory.createConnection(hbaseConf);

  val hbaseContext = new HBaseContext(sc, hbaseConf);

  val scanMR: Scan = new Scan();
  
  val tableClients = hbaseConn.getTable(TableName.valueOf("nsRec:clientes"));
  val tableBanners = hbaseConn.getTable(TableName.valueOf("nsRec:banners"));

  val scannerMR: ResultScanner = tableClients.getScanner(scanMR);

  val iterator = scannerMR.iterator()
  val familyName = "datos".getBytes()
  while (iterator.hasNext()) {
    //recorro la tabla de usser
    val fila: Result = iterator.next
    val idUser = fila.getRow
    var hasEdad = false
    var hasSexo = false
    if (fila.containsColumn(familyName, "edad".getBytes())) {

      hasEdad = true
    }
    if (fila.containsColumn(familyName, "sexo".getBytes())) {

      hasSexo = true
    }
    if (hasEdad && hasSexo) {
      //busco el banner en la tabla de banners

      val recEd = fila.getValue(familyName, "edad".getBytes());
      val edad: Integer = Integer.parseInt(String.valueOf(Bytes.toString(recEd)))
      val recSx = fila.getValue(familyName, "sexo".getBytes());
      val sexo: String = String.valueOf(Bytes.toString(recSx))

      val cadBuscar:String = (edad,sexo) match {
        case (x,y) if (x >=0 && x < 10) && sexo.equalsIgnoreCase("m") => "0-10m" 
        case (x,y) if (x >=10 && x < 20) && sexo.equalsIgnoreCase("m") => "10-20m"
        case (x,y) if (x >=20 && x < 30) && sexo.equalsIgnoreCase("m") => "20-30m" 
        case (x,y) if (x >=30 && x < 40) && sexo.equalsIgnoreCase("m") => "30-40m"
        case (x,y) if (x >=40 && x < 60) && sexo.equalsIgnoreCase("m") => "40-60m"
        case (x,y) if (x >=60 && x < 80) && sexo.equalsIgnoreCase("m") => "60-80m"
        case (x,y) if (x >=80 && x < 100) && sexo.equalsIgnoreCase("m") => "80-100m"  
          
        case (x,y) if (x >=0 && x < 10) && sexo.equalsIgnoreCase("f") => "0-10f" 
        case (x,y) if (x >=10 && x < 20) && sexo.equalsIgnoreCase("f") => "10-20f"
        case (x,y) if (x >=20 && x < 30) && sexo.equalsIgnoreCase("f") => "20-30f" 
        case (x,y) if (x >=30 && x < 40) && sexo.equalsIgnoreCase("f") => "30-40f"
        case (x,y) if (x >=40 && x < 60) && sexo.equalsIgnoreCase("f") => "40-60f"
        case (x,y) if (x >=60 && x < 80) && sexo.equalsIgnoreCase("f") => "60-80f"
        case (x,y) if (x >=80 && x < 100) && sexo.equalsIgnoreCase("f") => "80-100f"  
        case _                       => "empty"
      }
      
      if (!cadBuscar.equals("empty")){
        // hago un get de la tabla de banners con la cadena anterior
        val get1 = new Get(cadBuscar.getBytes());
        val filaBanner = tableBanners.get(get1);
        val qualBanner = filaBanner.getValue("banner".getBytes(), "cadBanner".getBytes());
        
        //inserto el banner en tabla clientes con nuevo qualifier
        val put1 = new Put(idUser);
        put1.addColumn("datos".getBytes(), "banner".getBytes(), qualBanner)
        tableClients.put(put1)
        println("Inserto banner de la tabla!")
      }

    } else {
      // inserto banner generico
      val cadByte = "banner generico".getBytes()
      val put1 = new Put(idUser);
      put1.addColumn("datos".getBytes(), "banner".getBytes(), cadByte)
      tableClients.put(put1)
      println("Inserto banner generico!")
    }

  }
  println("Actualizada tabla clientes con los banner!")

}