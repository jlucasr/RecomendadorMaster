package cargaDatos

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.hadoop.hbase.spark._
import java.util.Calendar
import java.text.SimpleDateFormat

object CargaHBase extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
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
  val desc = admin.listNamespaceDescriptors();
  var existe = false;
  for (a1: NamespaceDescriptor <- desc) {
    if (a1.getName.equals(nameSpaceN)) {
      existe = true
    }
  }
  if (!existe) {
    admin.createNamespace(nameSpace)
  }

  val tableName = TableName.valueOf("nsRec:clientes")
  val tabledescriptor = new HTableDescriptor(tableName);
  val nameFam = "datos".getBytes()
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

  val textEx = sc.textFile(s"hdfs://$hostName:8020/user/cloudera/recomendador/$cadNow/clientes")

  hbaseContext.bulkPut[String](textEx,
    tableName,
    (putRecord) => {
      val fields = putRecord.split('\t')
      val sizeFields = fields.size

      System.out.println("Voy a insertar:" + putRecord)
      val put = new Put(Bytes.toBytes(fields(0)))
      var hayCampos=false;
      
      if (sizeFields > 2) {
        val campo1 = fields(1)
        if (!campo1.isEmpty()) {
          hayCampos=true;
          put.addColumn(nameFam, "sexo".getBytes(), campo1.getBytes())
        }
      }
      if (sizeFields > 3) {
        val campo2 = fields(2)
        if (!campo2.isEmpty()) {
          hayCampos=true;
          put.addColumn(nameFam, "edad".getBytes(), campo2.getBytes())
        }
      }

      if (sizeFields > 4) {
        val campo3 = fields(3)
        if (!campo3.isEmpty()) {
          hayCampos=true;
          put.addColumn(nameFam, "pais".getBytes(), campo3.getBytes())
        }
      }
      if (sizeFields == 5) {
        val campo4 = fields(4)
        if (!campo4.isEmpty()) {
          hayCampos=true;
          put.addColumn(nameFam, "fecha".getBytes(), campo4.getBytes())
        }
      }
      if (!hayCampos){
        put.addColumn(nameFam, "default".getBytes(), "default".getBytes())
      }
      put
    });

}