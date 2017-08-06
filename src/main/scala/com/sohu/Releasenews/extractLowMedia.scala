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


object extractLowMedia {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wangteng_readfromHbase")
    sparkConf.set("spark.akka.frameSize","100")
    val sc = new SparkContext(sparkConf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val labelDicPath=argsMap.getOrElse("--labelDicPath", "")
    val out = argsMap.getOrElse("--output", "")

    val cfBadNews = HBaseConfiguration.create()
    cfBadNews.set(TableInputFormat.INPUT_TABLE,"BD_REC:NewsTable_N")
    HBaseUtilNew.setHBaseConfig(cfBadNews)

    val labelDic_br=sc.broadcast(sc.textFile(labelDicPath).map(x=>(x.split("\t")(1),x.split("\t")(0).split("_")(0))).collectAsMap())
    val title_keywords = sc.newAPIHadoopRDD(cfBadNews,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => x._2).map(x => {
      val notindex = 1<<5
      try{
        val title= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("title")))
        val media_name= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("media_name")))
        val keywords= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("meta_keywords")))
        val mmlabel= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmLabel")))
        val content= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("content")))
        val mmStatus= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmStatus"))).toInt
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

       val isLowMedia =if((notindex & mmStatus)!=0)true else false
        if(isLowMedia)
       {
         val labels=tags.map(x=>x.replace("v1.1_text_","")).map(x=>labelDic_br.value(x)).mkString("_")
          title+"\u0001"+media_name+"\u0001"+keywords+"\u0001"+labels+"\u0001"+content+"\u0001"+mmStatus
        }
       else {"null"}
      }
      catch {
        case e:Exception => "null"
      }
    }).filter(!_.contains("null")).saveAsTextFile(out)


  }
}
