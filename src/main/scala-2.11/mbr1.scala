import java.io.File
import java.sql.DriverManager
import java.util.Properties
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.bigdata.hive.udf.impl._
//import com.bigdata.hive.udf._

//import com.axagulf.spark.businessviews.utils.DateUtils

object mbr_read {

  var test1 = new com.bigdata.hive.udf.impl.HLSequenceGenerator()
  var cnt1 =test1.evaluate("seq1")
   def main(args: Array[String]): Unit = {

try
{

   def Seq_dist (Sname: String): Int = {

    current += 1;
     return  current 
   }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////Spark Context Declaration Body///////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val prop = new java.util.Properties()
    // val warehouseLocation = new File("hdfs://hdp-master.com:8020/warehouse/tablespace/managed/hive").getAbsolutePath
    val hiveLocation   = "hdfs://hdp-master.com:8020/warehouse"
    val sc = SparkSession.builder().appName("Load_Process")
      .master("local[1]")
      .config("hive.metastore.uris", "thrift://hdp-master.com:9083")
      .config("hive.metastore.warehouse.dir", "hdfs://hdp-master.com:8020/warehouse/tablespace/managed/hive")
      .config("spark.sql.warehouse.dir", hiveLocation)
      .enableHiveSupport()
      .getOrCreate
    var sqlContext = sc.sqlContext


 var dUtil  = new DJ() //com.axagulf.spark.businessviews.utils.DateUtils()

 var d=dUtil.julianIntegerToSQLTimestamp(1000)
    //  val sdf = new SimpleDateFormat("yyyy-MM-dd")
    // var date = sdf.parse("1500-01-01")

println("date :>>>>>>>>>>>>>>>>>>>>>>"+d)

println("date :>>>>>>>>>>>>>>>>>>>>>>1"+dUtil.julianIntegerToSQLTimestamp(1000612173))
println("date :>>>>>>>>>>>>>>>>>>>>>>2"+dUtil.julianIntegerToSQLTimestamp(2))
println("date :>>>>>>>>>>>>>>>>>>>>>>3"+dUtil.julianIntegerToSQLTimestamp(8103))

 
    ////----------------------------------------------------------------------------------------////
    ////*****************User Define Fucntion Registration*************************************
    ////----------------------------------------------------------------------------------------////
      sc.udf.register("dj" , dUtil.julianIntegerToSQLTimestamp _)

      sc.udf.register("ct" , dUtil.inc _)
sqlContext.sql(" use axa_raw")

val trg =sqlContext.sql("select count(1) as src_cnt from lob_mxem ")

trg.show

sqlContext.sql("use axa_refined")

var m2=sqlContext.sql("select count(1) as trg_cnt  from mbr_dim")

m2.show(10)


var m1=sqlContext.sql("select *  from mbr_dim")
m1.show(10)

var ut = new Util1()
ut.log_job( sqlContext,1,new java.sql.Timestamp(System.currentTimeMillis()),new java.sql.Timestamp(System.currentTimeMillis()),1) 


println("Reading hive table : OK")
sc.stop
}
catch
{

  case e: Exception =>  e.printStackTrace ;// sc.stop

}


  }

}