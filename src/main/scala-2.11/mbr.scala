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
//import com.axagulf.spark.businessviews.utils.DateUtils


object mbr_dim {
   def main(args: Array[String]): Unit = {

try
{
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

 println("date :>>>>>>>>>>>>>>>>>>>>>>"+d)

 // set UDFs
 sc.udf.register("ct" , dUtil.inc _)
 sc.udf.register("dj" , dUtil.julianIntegerToSQLTimestamp _)


 sqlContext.sql(" use axa_raw")


//date conversion
//val trg =sqlContext.sql("select ct(0) as MBR_DK , '-7777' as MBR_BK , '-7777' as  GND_TP_CD_DK , mxem_external_code as GND_TP_CD , mxem_class as GND_TP_DESCR , mxem_fst_name as FRST_NM , mxem_fam_name as LAST_NM , dj(mxem_dte_birth) as DOB , mxem_fst_name+' '+ mxem_fam_name FULL_NM , '-7777' as  CLNT_DK , mxem_external_code as MBR_CD , mxem_cchi_nationalid as EMPE_ID , '-7777' as  REL_TP_CD_DK , mxem_rel_grp as REL_TP_CD , ' ' as REL_TP_DESCR , '-7777' as  DEPT_TP_CD_DK , mxem_department as DEPT_TP_CD , mxem_department as DEPT_TP_DESCR , dj(mxem_entry_date) as VLD_FM_TS , dj(mxem_id_exp_date) as VLD_TO_TS , mxem_cchi_visanumber as CRN_IND , dj(mxem_prev_effective_date) as EFF_FM_DT , dj(mxem_prev_expiry_date) as EFF_TO_DT , '-7777' as LOAD_INFO_DK , '-7777' as LGL_OWN_CD_DK , '-7777' as SRC_CD_DK , NULL as ISRT_TS , NULL as UDT_TS , '-7777' as UDT_BY_USR_DK , '-7777' as CRT_BY_USR_DK   from lob_mxem ")

// nvl
//val trg =sqlContext.sql("select ct(0) as MBR_DK , '-7777' as MBR_BK , '-7777' as  GND_TP_CD_DK , mxem_external_code as GND_TP_CD , mxem_class as GND_TP_DESCR , mxem_fst_name as FRST_NM , mxem_fam_name as LAST_NM , nvl(dj(mxem_dte_birth),null) as DOB , mxem_fst_name+' '+ mxem_fam_name FULL_NM , '-7777' as  CLNT_DK , mxem_external_code as MBR_CD , mxem_cchi_nationalid as EMPE_ID , '-7777' as  REL_TP_CD_DK , mxem_rel_grp as REL_TP_CD , ' ' as REL_TP_DESCR , '-7777' as  DEPT_TP_CD_DK , mxem_department as DEPT_TP_CD , mxem_department as DEPT_TP_DESCR , nvl(dj(mxem_entry_date),null) as VLD_FM_TS , nvl(dj(mxem_id_exp_date),null) as VLD_TO_TS , mxem_cchi_visanumber as CRN_IND , nvl(dj(mxem_prev_effective_date),null) as EFF_FM_DT , nvl(dj(mxem_prev_expiry_date),null) as EFF_TO_DT , '-7777' as LOAD_INFO_DK , '-7777' as LGL_OWN_CD_DK , '-7777' as SRC_CD_DK , NULL as ISRT_TS , NULL as UDT_TS , '-7777' as UDT_BY_USR_DK , '-7777' as CRT_BY_USR_DK   from lob_mxem ")

//only dob date conversion
 val trg =sqlContext.sql("select ct(0) as MBR_DK , '-7777' as MBR_BK , '-7777' as  GND_TP_CD_DK , mxem_external_code as GND_TP_CD , mxem_class as GND_TP_DESCR , mxem_fst_name as FRST_NM , mxem_fam_name as LAST_NM , nvl(dj(mxem_dte_birth),NULL) as DOB , mxem_fst_name+' '+ mxem_fam_name FULL_NM , '-7777' as  CLNT_DK , mxem_external_code as MBR_CD , mxem_cchi_nationalid as EMPE_ID , '-7777' as  REL_TP_CD_DK , mxem_rel_grp as REL_TP_CD , ' ' as REL_TP_DESCR , '-7777' as  DEPT_TP_CD_DK , mxem_department as DEPT_TP_CD , mxem_department as DEPT_TP_DESCR , NULL as VLD_FM_TS , NULL as VLD_TO_TS , mxem_cchi_visanumber as CRN_IND , NULL as EFF_FM_DT , NULL as EFF_TO_DT , '-7777' as LOAD_INFO_DK , '-7777' as LGL_OWN_CD_DK , '-7777' as SRC_CD_DK , NULL as ISRT_TS , NULL as UDT_TS , '-7777' as UDT_BY_USR_DK , '-7777' as CRT_BY_USR_DK   from lob_mxem ")


sqlContext.sql("use axa_refined")


trg.write.mode("Overwrite").insertInto("MBR_DIM")


var m2=sqlContext.sql("select * from mbr_dim")

m2.show(30)


println("Reading hive table : OK")
sc.stop
}
catch
{

  case e: Exception =>  e.printStackTrace ;// sc.stop

}


  }

}