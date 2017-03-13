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
  
  // Spark context initialization
  val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
  val sc = new SparkContext(conf)
  
  // Host name input
  val hostName = "quickstart.cloudera"  

  // HBase configuration
  val hbaseConf = HBaseConfiguration.create()  
  hbaseConf.set("hbase.zookeeper.quorum", hostName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
  // HBase configuration with zookeeper node
  hbaseConf.set("zookeeper.znode.parent", "/hbase");

  val hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  val hbaseContext = new HBaseContext(sc, hbaseConf);

  // Name space input
  val nameSpaceN = "nsmrec"
  
  val admin = hbaseConn.getAdmin()
  val nameSpace = NamespaceDescriptor.create(nameSpaceN).build();
  val desc =  admin.listNamespaceDescriptors();
  
  // Check if name space is created
  var exists = false;
  for( a1:NamespaceDescriptor <- desc) {
    if (a1.getName.equals(nameSpaceN)) {
      exists=true  
    }
  }
  
  // If this name space is not created, then create it
  if (!exists) {
     admin.createNamespace(nameSpace)
  } 
  
  // Table name input
  val tableName = "users"
  val nsAndTableName = TableName.valueOf(nameSpaceN + ":" + tableName)
  val tabledescriptor = new HTableDescriptor(nsAndTableName);
  
  // Column family name input
  val columnFamilyName = "userData".getBytes()
  
  val familyData = new HColumnDescriptor(columnFamilyName);
  tabledescriptor.addFamily(familyData);
  familyData.setMaxVersions(1)
  // If table exists, then not create it
  if (!admin.tableExists(nsAndTableName)) {
    admin.createTable(tabledescriptor);
  }
  val table = hbaseConn.getTable(nsAndTableName);
  
  // Get data now
  val now = Calendar.getInstance().getTime()
  val formatter = new SimpleDateFormat("YYYY-MM-dd")
  val stringNow = formatter.format(now)

  // Users HDFS directory input
  val usersDirectory = "mrec/users/"
  val textEx = sc.textFile(s"hdfs://quickstart.cloudera:8020/user/cloudera/" + usersDirectory + "/$stringNow")
  //val textEx = sc.textFile(s"hdfs://quickstart.cloudera:8020/user/cloudera/" + usersDirectory + "2017-03-10")

  hbaseContext.bulkPut[String] (
    textEx,
    nsAndTableName,
    (putRecord) => {
      val fields = putRecord.split('\t')
      
      System.out.println("Put record: " + putRecord)
      
      val put = new Put(Bytes.toBytes(fields(0)))
      
      val fieldsSize = fields.size
      
      if (fieldsSize >= 2)
      {
        val genderField = fields(1)
        if (!genderField.isEmpty()) {
          put.addColumn(columnFamilyName, "gender".getBytes(), genderField.getBytes())
        }
      }
      
      if (fieldsSize >= 3)
      {
      val ageField = fields(2)
      if (!ageField.isEmpty()) {
        put.addColumn(columnFamilyName, "age".getBytes(), ageField.getBytes())
      }
      }
      
      if (fieldsSize >= 4)
      {
      val countryField = fields(3)
      if (!countryField.isEmpty()) {
        put.addColumn(columnFamilyName, "country".getBytes(), countryField.getBytes())
      }
      }
      
      if (fieldsSize == 5)
      {
      val registerDateField = fields(4)
      if (!registerDateField.isEmpty()) {
        put.addColumn(columnFamilyName, "register date".getBytes(), registerDateField.getBytes())
      }
      }
      
      // If there are not fields, then add column family default
      if (put.size == 0)
      {
        put.addColumn(columnFamilyName, "default".getBytes(), "default".getBytes())
      }
      
      put
    } );


}