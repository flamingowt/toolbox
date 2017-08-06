package com.sohu.tool_box

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

/**
 * Created by fengzeli210057 on 2017/5/27.
 */
object readFromHbase {

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
    val badNewsMap = sc.newAPIHadoopRDD(cfBadNews,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => x._2).map(x => {
      try{
        val newsid=Bytes.toString(x.getRow)
        val mmlabel= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmLabel")))
        val title= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("title")))
        val content= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("content")))

        val newsTag_text = try {
          LabelParse.StringToMap(mmlabel).get("v1.1_text")
        } catch {
          case e: Exception => null
        }
        var tags = ArrayBuffer[String]()
        if (null != newsTag_text) {
          for (i <- 0 until newsTag_text.size()) {
            val tag = newsTag_text.get(i)
            if (null != tag) {
              tags += ((tag.getKey))
            }
          }
        }
        newsid+"\u0001"+title+"\u0001"+tags.mkString("_")+"\u0001"+content
      }
catch {
  case e:Exception => ("")
}
    }).filter(!_.equals("")).filter(_.split("\u0001").length>3).filter(x=>x.split("\u0001")(1).contains("发布")||x.split("\u0001")(1).contains("颁布")||x.split("\u0001")(1).contains("发行")||x.split("\u0001")(1).contains("宣传")||x.split("\u0001")(1).contains("宣布"))
  .saveAsTextFile(out)

  }
}
