//package com.axagulf.spark.businessviews.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Project Name spark-businessViews
  * Created by Mohsin Hashmi on 7/11/17.
  */
//object DJ{ //DateUtils {

class DJ extends Serializable  { //DateUtils {


   private var current = 0

 def inc (julianInt: Int): Int = {

    current += 1;
     return  current 
   }


  def julianIntegerToDate (julianInt: Int): Date = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse("0001-01-01")
    val calender = Calendar.getInstance()
    calender.setTime(date)
    val days = julianInt - 1721424

      calender.add(Calendar.DATE, days)

    calender.getTime
  }

  def julianIntegerToSQLDate (julianInt: Int): java.sql.Timestamp = {
     new java.sql.Timestamp(julianIntegerToDate(julianInt).getTime)
  }


  def julianIntegerToSQLTimestamp (julianInt: Int): java.sql.Timestamp = {
   new java.sql.Timestamp(julianIntegerToDate(julianInt).getTime)
  }

  def julianIntegerToDateString (julianInt: Int): String = {

    dateToString(julianIntegerToDate(julianInt))
  }

  def stringToDate (stringDate: String): Date = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var date = sdf.parse(stringDate)
    date
  }

  def dateToString (date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var strDate = sdf.format(date)
    strDate
  }

  def getMonthString(date: Timestamp): String = {
    val sdf = new SimpleDateFormat("MMMM")
    var strDate = ""
    if(date != null)
      strDate = sdf.format(date)
    strDate
  }

  def getYearNumber(date: Timestamp): Int = {
    val sdf = new SimpleDateFormat("YYYY")
    var strDate = "0"
    if(date != null)
      strDate = sdf.format(date)
    strDate.toInt
  }

  def dateDifferenceInDays(startDate: Timestamp,endDate: Timestamp): Long = {
    var diffDays = 0L
    if(startDate != null && endDate != null)
      {
        val diffTime = startDate.getTime() - endDate.getTime()
        diffDays = diffTime / (1000 * 60 * 60 * 24)
      }
    diffDays
  }

  def getCurrentTimestamp(): Timestamp =  {

    new Timestamp(Calendar.getInstance().getTimeInMillis)
  }
}
