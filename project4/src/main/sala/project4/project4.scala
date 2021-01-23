package project4
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.spark.avro
import com.mysql.cj.jdbc.Driver
import java.io.File
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog}
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
object project4 {
  
def main(args:Array[String]):Unit={
  val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc   .setLogLevel("Error")
    val spark = SparkSession.builder().appName("HBASE-DYNAMIC")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport().master("local[*]").getOrCreate()			
			import spark.implicits._
			val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
			import hiveContext._	
			
			val hc = new HiveContext(sc)
  val metadf=spark.read.format("jdbc").option("header","True").option("url","jdbc:mysql://localhost:3306/zeyo?useLegacyDatetimeCode=false&serverTimezone=UTC").option("driver","com.mysql.jdbc.Driver").option("dbtable","r_meta").option("user","root")
            .option("password","cloudera").load()  
   println("1.data collected")         
   val data=metadf.collect.toList
   //println(data.map(x =>x.get(1)))
 
  println(data)
  val cols =data.map{x =>s""""${x.get(1)}":{"cf":"cf","col":"${x.get(0)}","type":"string"}""".stripMargin}
  println(cols)
  val catalog=s"""{
  |"table":{"namespace":"default", "name":"hbase_tract10"},
  |"rowkey":"rowkey",
  |"columns":{
  |"id":{"cf":"rowkey", "col":"rowkey", "type":"string"},
  |${cols.mkString(",\n")}
|}
|}""".stripMargin 
  
 // println(catalog)
  
val hbasedata=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .load()  
    hbasedata.show()
  println("1.hbase dataloaded")

//val hidata=hbasedata.select(hbasedata.columns.filter(_.startsWith("BXCBLL")).map(hbasedata(_)) : _*).show
val colsplit = hbasedata.columns.map(x => x.split("_")(0)).distinct.drop(1)
var ln=colsplit.length

for(i <- 0 to ln-1)
{
   //val df1 = hbasedata.filter(x -> x.contains(colsplit(i))
  val df = hbasedata.select(hbasedata.columns.filter(_.startsWith(colsplit(i))).map(hbasedata(_)) : _*)
  df.show()
  val hivetable=df.write.format("hive").saveAsTable(colsplit(i)+ "_tb")
}

//First grab the column names with df.columns, then filter down to just the column names you want .filter(_.startsWith("colF")). 
//This gives you an array of Strings. But the select takes select(String, String*). Luckily select for columns is select(Column*),
//so finally convert the Strings into Columns with .map(df(_)), and finally turn the Array of Columns into a var arg with : _*.

//// Simple scala way of mapping
//df.select("id").collect().map(_(0)).toList
//res15: List[Any] = List(one, two, three)
 }   
}

