package com.sohu.news.longLabelNegativeFeedback

import java.io.ByteArrayInputStream
import java.net.URI

import com.github.nscala_time.time.Imports._
import com.sohu.util._
import com.sohu.proto.rawlog.Log
import com.sohu.avro.dao.Event
import com.sohu.news.common.parse.LabelParse
import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by linbai on 2017/5/18.
  */
object extractUserBehavior {//
  val DELIMA = "\u0001"

  def parseLog(v:Array[Byte]) : Array[(Int, String, Array[String],Int,Int)] = {
    val input = new ByteArrayInputStream(v)
    val data = new ArrayBuffer[(Int, String, Array[String],Int,Int)]()

    while (input.available() > 0) {
      val device = try {
        Log.parseDelimitedFrom(input)
      } catch {
        case e: Exception =>
          null
      }

      if (device != null) {
        val uid = device.getUid.getDid
        val eventid = device.getEvent.getNumber
        val newsids = device.getEntityDetail().getContentIdList.toArray().mkString(",").split(",")
        val pagesourceNum=device.getActionFrom.getPageSource.getNumber
        val os = device.getPhoneMeta.getOs.getNumber
        val len = newsids.length
        if (len > 0 && uid != null && uid.length > 0 && pagesourceNum!=null) {
          if (Constants.NEWSREC_EVENT_CLICK == eventid)
            data += ((1, uid, newsids,pagesourceNum,os))
          else if (Constants.NEWSREC_EVENT_PV == eventid)
            data += ((0, uid, newsids,pagesourceNum,os))
        }
      }
    }
    input.close()
    data.toArray
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("yongqiliu:CalcBehaviorStatistics") //.setMaster("local[3]")
    sparkConf.set("spark.akka.frameSize", "2047")
    sparkConf.set("spark.kryoserializer.buffer.max.mb", "2040")
    sparkConf.set("spark.files.overwrite", "true")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    sparkConf.set("spark.eventLog.overwrite", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.set("spark.kryo.registrator", "com.sohu.liuyq.util.MyRegistrator")
    sparkConf.registerKryoClasses(Array(classOf[Event]))
    val sc = new SparkContext(sparkConf)

    val argsMap = ParseArgs.parse(args)

    val logInput = argsMap.getOrElse("--input", "")//原始日志输入
    val output = argsMap.getOrElse("--output", "")
    val beginDay = argsMap.getOrElse("--beginDay", "")
    val days = argsMap.getOrElse("--days", "").toInt

    val config = new Configuration()
    val readJob = new Job(config)
    AvroJob.setInputKeySchema(readJob, Event.getClassSchema)

    val dateTo = DateTime.parse(beginDay, DateTimeFormat.forPattern("yyyyMMdd"))
    for (i <- 0 until days) {//(eventid, uid, newsids,pagesourceNum)
      // 读用户
      try {
        var targetDateInputDir = ""
        if (logInput.endsWith("/"))
          targetDateInputDir = logInput + (dateTo - i.days).toString("yyyy/MM/dd") + "/*/*.avro"
        else
          targetDateInputDir = logInput + "/" + (dateTo - i.days).toString("yyyy/MM/dd") + "/*/*.avro"
        val out = output + "/raw/" + (dateTo - i.days).toString("yyyyMMdd")
        val avroRDD = sc.newAPIHadoopFile(targetDateInputDir,
          classOf[AvroKeyInputFormat[Event]],
          classOf[AvroKey[Event]],
          classOf[NullWritable],
          readJob.getConfiguration).flatMap(l => {
          var message = new Array[Byte](0)
            val buf = l._1.datum.getBody
            message = buf.array()
          parseLog(message)//0, uid, newsids,pagesourceNum,os
        }).cache()
        avroRDD.repartition(1).filter(_._5==1).map(x => x._1 + "\t" + x._2 + "\t" + x._3.mkString(",")+"\t"+ x._4).saveAsTextFile(out)
        //(eventid, uid, newsids,pagesourceNum)
      } catch {
        case e: Exception =>
          println(e.getStackTrace.mkString("|") + "\t" + " ====> " + e.getMessage)
      }
    }
    val data = sc.textFile(output+"/raw/*/p*").cache()
   // data.repartition(10).saveAsTextFile(output + "/data")

    val user_id_pv_clk = data.filter(_.split("\t").length == 4).flatMap(x =>{
      val pv_clk = x.split("\t")(0)
      val user = x.split("\t")(1)
      val pagesourceNum=x.split("\t")(3)
      val ids = x.split("\t")(2).split(",").map(y => user + "@" + y + "\t" + pv_clk + "\t"+pagesourceNum).toArray
      ids
    })
    user_id_pv_clk.repartition(20).saveAsTextFile(output + "/user_id_pv_clk_pagesourceNum")

    //sc.textFile(output + "/user_id_pv_clk_pagesourceNum/*")

  }
}
