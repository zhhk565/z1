//package com.axagulf.spark.businessviews.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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


/**
  * Project Name spark-businessViews
  * Created by Mohsin Hashmi on 7/11/17.
  */
class Util1{ //DateUtils {

//class DJ extends Serializable  { //DateUtils {


   private var current = 0

 def inc (julianInt: Int): Int = {

    current += 1;
     return  current 
   }

//@return 1 on success and 0 on failure
def log_job( sqlContext: SQLContext ,jobid:Int,starttime :java.sql.Timestamp,endtime : java.sql.Timestamp,jobstatus : Int ) : Int = 
{

import sqlContext.implicits._
try {

var job1 = sqlContext.createDataFrame(Seq(
(jobid, starttime,endtime,jobstatus)
)).toDF("jobid","starttime" ,"endtime" , "job_status")

//job1.write
job1.write.format("orc").mode(SaveMode.Append).option("path", "/user/Axa-Datalake/refined_zone/")
.saveAsTable("axa_refined.jobs_log")
return 1
} catch {
case e: Exception => return 0 //e.printStackTrace

}
}

}
