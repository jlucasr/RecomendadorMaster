package cargaDatos

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.hadoop.hbase.spark._
import java.util.Calendar
import java.text.SimpleDateFormat 

  /*
   *  leer el fichero de la ruta hdfs de los banner y cargarlos en una tabla HBASE
   */
object CargaBannerHBase extends App  {
  

  
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

  val admin = hbaseConn.getAdmin()
  val nameSpaceN = "nsRec"
  val nameSpace = NamespaceDescriptor.create(nameSpaceN).build();
  val desc =  admin.listNamespaceDescriptors();
  var existe = false;
  for( a1:NamespaceDescriptor <- desc){
    if (a1.getName.equals(nameSpaceN)){
      existe=true  
    }
  }
  if (!existe){
     admin.createNamespace(nameSpace)
  }
 
  //crear la tabla de banner si no existe
  val tableName = TableName.valueOf("nsRec:banners")
  val tabledescriptor = new HTableDescriptor(tableName);
  val nameFam = "banner".getBytes()
  val familyDatos = new HColumnDescriptor(nameFam);
  tabledescriptor.addFamily(familyDatos);
  familyDatos.setMaxVersions(1)
  if (!admin.tableExists(tableName)) {
    admin.createTable(tabledescriptor);
  }
  val table = hbaseConn.getTable(tableName);
  
  // fecha de hoy
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val cadNow = formatter.format(now)
  
  val ruta = s"hdfs://quickstart.cloudera:8020/user/cloudera/recomendador/$cadNow/banners"
  val textEx = sc.textFile(ruta)
  
   hbaseContext.bulkPut[String](textEx,
    tableName,
    (putRecord) => {
      val fields = putRecord.split(',')
      System.out.println("Voy a insertar:" + putRecord)
      
      val put = new Put(Bytes.toBytes(fields(0)))    
      val campo1 = fields(1)
      put.addColumn(nameFam, "cadBanner".getBytes(), campo1.getBytes())
      put
    });
  
}