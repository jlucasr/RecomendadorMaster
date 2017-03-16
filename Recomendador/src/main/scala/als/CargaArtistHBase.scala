package als

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkContext, SparkConf}
import org.apache.spark.rdd._
import org.apache.hadoop.hbase.spark._
import java.util.Calendar
import java.text.SimpleDateFormat 
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, SQLContext ,Row}
import org.apache.spark.ml.recommendation.{ ALSModel, ALS }

case class Artist (val artidnum:Int, val artid: String, val artname: String)

object CargaArtistHBase extends App{
  
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("Recomendador Spark")
  val sc = new SparkContext(conf)
 

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
  val tableName = TableName.valueOf("nsRec:artistWithName")
  val tabledescriptor = new HTableDescriptor(tableName);
  val nameFam = "datos".getBytes()
  val familyDatos = new HColumnDescriptor(nameFam);
  tabledescriptor.addFamily(familyDatos);
  familyDatos.setMaxVersions(1)
  if (!admin.tableExists(tableName)) {
    admin.createTable(tabledescriptor);
  }
  val table = hbaseConn.getTable(tableName);
  
  val ruta = s"hdfs://$hostName:8020/user/cloudera/mrec/artIdNum"
  val artistNames = sc.textFile(ruta)
  
  
  hbaseContext.bulkPut[String](artistNames,
    tableName,
    (putRecord) => {
      val fields = putRecord.split('\t')
      val clave = fields(0)
      val artid = fields(1)
      val artname = fields(2)
    
      println(s"Voy a insertar: $clave, $artid, $artname")
      
      val put = new Put(Bytes.toBytes(clave))       
      put.addColumn(nameFam, "artid".getBytes(), artid.getBytes())
      put.addColumn(nameFam, "artname".getBytes(), artname.getBytes())
      put   
      
   });
  
  
}