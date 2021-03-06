package com.sohu.Releasenews

import com.sohu.news.common.parse.LabelParse
import com.sohu.news.rec.ACAutomation
import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import utils.HBaseUtil.HBaseUtilNew

import scala.collection.mutable.ArrayBuffer


object extractLocalMedia {//媒体名包含地名、标题包含发布宣传等标语的新闻

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("wangteng_readfromHbase")
    sparkConf.set("spark.akka.frameSize","100")
    val sc = new SparkContext(sparkConf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val labelDicPath=argsMap.getOrElse("--labelDicPath", "")
    val AC_local_path=argsMap.getOrElse("--AC_local_path", "")
    val AC_releaseRule_path=argsMap.getOrElse("--AC_releaseRule_path", "")
    val out = argsMap.getOrElse("--output", "")

    val AC_local_dic_br=sc.broadcast(sc.textFile(AC_local_path).collect().mkString(","))
    val AC_releaseRule_dic_br=sc.broadcast(sc.textFile(AC_releaseRule_path).collect().mkString(","))
    val cfBadNews = HBaseConfiguration.create()
    cfBadNews.set(TableInputFormat.INPUT_TABLE,"BD_REC:NewsTable_N")
    HBaseUtilNew.setHBaseConfig(cfBadNews)

    val labelDic_br=sc.broadcast(sc.textFile(labelDicPath).map(x=>(x.split("\t")(1),x.split("\t")(0).split("_")(0))).collectAsMap())
    val title_keywords = sc.newAPIHadoopRDD(cfBadNews,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => x._2).mapPartitions(p => {
      val AC_local=new ACAutomation(AC_local_dic_br.value)

      val attr=ArrayBuffer[String]()
      while(p.hasNext) {
        val x=p.next()
        try {
          val title = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("title")))
          val media_name = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("media_name")))
          val keywords = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("meta_keywords")))
          val mmlabel = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmLabel")))
          val content = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("content")))
          // val mmStatus= Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("mmStatus"))).toInt
          val read_count = Bytes.toString(x.getValue(Bytes.toBytes("SAttrs"), Bytes.toBytes("read_count"))).toInt
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
          if (AC_local.find(media_name).size() > 0) {
            val labels = tags.map(x => x.replace("v1.1_text_", "")).map(x => labelDic_br.value(x)).mkString("_")
            attr.append(title + "\u0001" + media_name + "\u0001" + keywords + "\u0001" + labels + "\u0001" + content + "\u0001" + read_count)
          }
        }
        catch {
          case e: Exception => attr.append("null")
        }
      }
      attr.toIterator
    }).filter(!_.contains("null")).saveAsTextFile(out+"/localmedia")

      sc.textFile(out+"/localmedia").filter(_.split("\u0001",2).length>1)
        .mapPartitions(x=>{
          val AC_releaseRule=new ACAutomation(AC_releaseRule_dic_br.value)
          val attr=ArrayBuffer[String]()
          while (x.hasNext){
            val line=x.next()
            if(AC_releaseRule.find(line.split("\u0001")(0)).size()>0)
              attr.append(line)
          }
          attr.toIterator
        }).saveAsTextFile(out+"/releaseNews")


  }
}
