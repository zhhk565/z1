import java.io.File
import java.sql.DriverManager
import java.util.Properties
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
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
//import _root_.kafka.serializer.DefaultDecoder
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object load_p {
   def main(args: Array[String]): Unit = {

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

    def log_job( jobid:Int,starttime :java.sql.Timestamp,endtime : java.sql.Timestamp,jobstatus : Int ) : Int = {
 
     try {
 
              var job1 = sqlContext.createDataFrame(Seq(
            (jobid, starttime,endtime,jobstatus)
                  )).toDF("jobid","starttime"  ,"endtime"  , "job_status")

          //job1.write
          job1.write.format("orc").mode(SaveMode.Append).option("path", "/user/Axa-Datalake/refined_zone/")
              .saveAsTable("axa_refined.jobs_log")
      return 1
        } catch {
          case e: Exception =>  return 0 //e.printStackTrace
        
      }
   }  


  // import org.apache.spark.sparkSession.implicits._
 //  import sparkSession.sql
val list1 =sqlContext.sql("show databases like '*.*' ")
list1.show()
log_job(1,new java.sql.Timestamp(System.currentTimeMillis()),new java.sql.Timestamp(System.currentTimeMillis()+10000),1)
sqlContext.sql("use curated_zone")

var tabs=sqlContext.sql("SHOW TABLES ")

tabs.show()
log_job(2,new java.sql.Timestamp(System.currentTimeMillis()),new java.sql.Timestamp(System.currentTimeMillis()+10000),1)


// var fact=sqlContext.sql ("select * from curated_zone.fact")

// fact.show

// var cust=sqlContext.sql ("select * from curated_zone.cust")

// cust.show


sqlContext.sql("use raw_zone")

 tabs=sqlContext.sql("SHOW TABLES ")

tabs.show()

var endtime=new java.sql.Timestamp(System.currentTimeMillis())
var starttime=new java.sql.Timestamp(System.currentTimeMillis())

//(batchid,jobid,func_area,target_table, starttime1,endtime,src_cnt.toIn,tgt_cnt.toInt,job_status,
  var script="select * from batch_jobs"
  var batchid=1
  var jobid=1
  var func_area="AXA"
  var  target_table="target1"
  var src_cnt=1
  var trg_cnt=1
  var job_status=3

var j1=sqlContext.sql ("select * from batch_jobs")
j1.show()

/*
  var job1 = sqlContext.createDataFrame(Seq(
  (batchid,jobid,func_area,target_table, starttime,endtime,src_cnt,trg_cnt,job_status,script)
        )).toDF("batchid","jobid","func_area" ,"target_table" ,"starttime"  ,"endtime"  ,"source_tb_count" , "target_tb_count" , "job_status","script")


var result=sqlContext.sql(
"CREATE TABLE if not exists batch_jobs (batchid int ,"+
"  jobid int ,func_area String,target_table String,"+
"  starttime timestamp,"+
"   endtime timestamp , "+
"   source_tb_count int, target_tb_count int, "+
"  job_status int,script String"+
//"  -- PRIMARY KEY (jobid)"+
"  )"+
" STORED AS ORC TBLPROPERTIES ('transactional'='true')"
)

result.show()
*/

//job1.show
//job1.write.mode("append").jdbc(def_jdbcUrl, "job_status", connectionProperties)
//job1.write.mode(SaveMode.Overwrite).saveAsTable("raw_zone.batch_jobs")

/*
    sqlContext.sql("DROP TABLE IF EXISTS raw_zone.batch_jobs")
    job1.write.option("path", s"hdfs://hdp-master.com:8020/user/Hive-Datalake/Raw_Zone/")
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .saveAsTable("raw_zone.batch_jobs")
*/

log_job(3,new java.sql.Timestamp(System.currentTimeMillis()),new java.sql.Timestamp(System.currentTimeMillis()+10000),1)

var jobs=sqlContext.sql("select * from axa_refined.jobs_log")

jobs.show()

// var customer=sqlContext.sql ("select * from customers")
// var cust_type =sqlContext.sql ("select * from cust_type")

//  customer.show()
//  cust_type.show()

//val df = sc.createDataFrame(Seq("First Hive Table")).toDF().coalesce(1)
 
// ======= Writing files
// Writing Dataframe as a Hive table
 
//sc.sql("DROP TABLE IF EXISTS test1")
//sc.sql("CREATE TABLE test1 (message STRING)")
//df.write.mode(SaveMode.Overwrite).saveAsTable("test1")

// Reading hive table into a Spark Dataframe
//val dfHive = sc.sql("SELECT * from test1")
println("Reading hive table : OK")
//println(dfHive.show())


sc.stop
  }

}