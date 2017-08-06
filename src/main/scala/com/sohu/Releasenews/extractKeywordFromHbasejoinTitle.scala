package com.sohu.Releasenews

import com.sohu.news.common.parse.LabelParse
import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import utils.HBaseUtil.HBaseUtilNew

import scala.collection.mutable.ArrayBuffer


object extractKeywordFromHbasejoinTitle {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wangteng_readfromHbase")
    sparkConf.set("spark.akka.frameSize","100")
    val sc = new SparkContext(sparkConf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val out = argsMap.getOrElse("--output", "")

    val cfBadNews = HBaseConfiguration.create()
    cfBadNews.set(TableInputFormat.INPUT_TABLE,"BD_REC:NewsTable_N")
    HBaseUtilNew.setHBaseConfig(cfBadNews)
    val title_keywords = sc.newAPIHadoopRDD(cfBadNews,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => x._2).map(x => {
      try{
        val title= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("title")))
        val keywords= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("meta_keywords")))
        (title,keywords)
      }
      catch {
          case e:Exception => ("","")
      }
    }).repartition(1).join(sc.textFile(input).map(x=>(x,"")).repartition(1)).map(x=>x._1+"\u0001"+x._2._1).saveAsTextFile(out)


  }
}
