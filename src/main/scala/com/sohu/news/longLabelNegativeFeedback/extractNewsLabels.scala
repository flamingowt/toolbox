package com.sohu.news.longLabelNegativeFeedback

/**
  * Created by linbai on 2017/5/19.
  */

import com.sohu.utils.HBaseUtil.ParseArgs

import scala.collection.mutable.ArrayBuffer
import utils.HBaseUtil.Constants._
import com.sohu.news.common.parse.LabelParse
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.Get

object extractNewsLabels {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("get news keyword")
    conf.set("spark.akka.frameSize", "2047")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //一些默认的类使用kryo序列化
    conf.set("spark.kryoserializer.buffer.max.mb", "2040")
    conf.set("spark.files.overwrite", "true")
    conf.set("spark.hadoop.validateOutputSpecon", "false")
    conf.set("spark.eventLog.overwrite", "true")
    conf.set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("dfs.datanode.socket.write.timeout", "180000000")
    sc.hadoopConfiguration.set("dfs.client.socket-timeout", "180000000")
    sc.hadoopConfiguration.set("dfs.socket.timeout", "180000000")

    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val out = argsMap.getOrElse("--output", "")
    val repartion = argsMap.getOrElse("--repartion", "").toInt


    val rawdata=sc.textFile(input).filter(_.split("\t").length==3).filter(_.split("\t")(0).split("@").length==2).repartition(repartion).mapPartitions(_.map(x=>{
      val cf = HBaseConfiguration.create()
      //utils.HBaseUtil.HBaseUtilNew.setHBaseConfig(cf)
      utils.HBaseUtil.HBaseUtilNew.setHBaseConfig(cf)
      val table = utils.HBaseUtil.HBaseClient.getConnection(cf).getTable("BD_REC:NewsTable_N")
      val newsid = x.split("\t")(0).split("@")(1)
      val click_pv = x.split("\t")(1)
      val hbaseKey = new Get(Bytes.toBytes(newsid))
      if(!table.exists(hbaseKey)){
        ""
      }else{
        val hBaseRow = table.get(hbaseKey)
        val mmlabel = Bytes.toString(hBaseRow.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmLabel")))
        //val newsType = Bytes.toString(hBaseRow.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("type")))

        var tags = ArrayBuffer[String]()

        /*
        val videoTag = try {
          LabelParse.StringToMap(mmlabel).get("v1.0_video")
        } catch {
          case e: Exception => null
        }
        if (null != videoTag) {
          for (i <- 0 until videoTag.size()) {
            val tag = videoTag.get(i)
            if (null != tag) {
              tags += ((tag.getKey))
            }
          }
        }
        */

        val newsTag_text = try {
          LabelParse.StringToMap(mmlabel).get("v1.1_text")
        } catch {
          case e: Exception => null
        }
        val newsTag_video = try {
          LabelParse.StringToMap(mmlabel).get("v1.1_video")
        } catch {
          case e: Exception => null
        }
        if (null != newsTag_text) {
          for (i <- 0 until newsTag_text.size()) {
            val tag = newsTag_text.get(i)
            if (null != tag) {
                tags += ((tag.getKey))
            }
          }
        }
        if (null != newsTag_video) {
          for (i <- 0 until newsTag_video.size()) {
            val tag = newsTag_video.get(i)
            if (null != tag) {
              tags += ((tag.getKey))
            }
          }
        }
       click_pv+"\t"+ newsid +"\t" + tags.toArray.mkString(",")
      }
    })).saveAsTextFile(out)

/*
    rawdata.filter(_.split("\t",2)(0).equals("1")).saveAsTextFile(out+"/click")
    rawdata.filter(_.split("\t",2)(0).equals("0")).saveAsTextFile(out+"/pv")*/
  }
}
