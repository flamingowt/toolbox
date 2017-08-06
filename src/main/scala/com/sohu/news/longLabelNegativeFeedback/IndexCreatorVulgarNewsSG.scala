/*
package com.sohu.news.longLabelNegativeFeedback

import java.util

import com.sohu.news.common.parse.LabelParse
import com.sohu.news.common.po.{ScoreMap, TagInvertedDetail, TagInvertedIndexPO}
import com.sohu.news.common.util.{NewsRecDaoUtil, RecTagMappingParseUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import utils.HBaseUtil.HBaseUtilNew

/**
 * Created by fengzeli210057 on 2017/5/27.
 */
object IndexCreatorVulgarNewsSG {
  def getTuple(newsInfo : Result,editorTagMappingBr:Broadcast[util.Map[String,util.List[String]]]):(String,Long,Boolean,Boolean,Boolean,Boolean,Int,Long) = {
    try{
      val newsid = Bytes.toString(newsInfo.getRow)
      val typeS = Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("type"))).toInt
      val newsTagFamily = Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("mmLabel")))
      val createTime = Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("meta_oriCreateTime"))).toLong
      val currentTime = System.currentTimeMillis()
      val readCount = try{
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("read_count"))).toLong
      }catch {
        case e : Exception => 0
      }
      val commentCount = try{
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("comment_count"))).toLong
      }catch {
        case e : Exception => 0
      }
      val sumReadCount = readCount + 5*commentCount
      val newsSite = try {
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("site"))).toInt //新闻来源，目前只推取值为19的微信的文章，因为比较三俗
      } catch {
        case e : Exception => -1
      }
      val status = try {
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("meta_status"))).toInt //是否是重复新闻，0表示不是重复的新闻，1表示该文章是重复的新闻，不用推荐
      } catch {
        case e : Exception => -1
      }
      val template = try {
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("template"))).toInt //模板，4是无图模板，暴力三俗的倒排不入无图的新闻
      } catch {
        case e : Exception => -1
      }
      val vulgar_type = try{
        Bytes.toString(newsInfo.getValue(Bytes.toBytes("SAttrs"),Bytes.toBytes("vulgar_type"))).toInt //三俗类型,1111分别表示 自己三俗|暴力|写真|搜狗三俗
      }catch {
        case e : Exception => 1
      }
      val isVulgarSogou = if ((vulgar_type & (1<<3)) != 0) true else false
      val isVulgarPic = if ((vulgar_type & (1<<1)) != 0) true else false
      val isVulgarViolence = if ((vulgar_type & (1<<2)) != 0) true else false
      val newsTypeKey = if (typeS == 1){
        //"v1.0_text"
        //"v1.1_text"
        Constant.textTagVersion
      } else {
        //"v1.0_video"
        //"v1.1_video"
        Constant.videoTagVersion
      }
      val newsTag = try {
        LabelParse.StringToMap(newsTagFamily).get(newsTypeKey)
      } catch {
        case e : Exception => new util.ArrayList[ScoreMap]()
      }
      var isLegalVulgarNews = false
      //if (newsTag != null && (createTime+7*24*60*60*1000>currentTime)){
      if (newsTag != null && newsSite != 1 && isVulgarSogou && template != 4){
        for (i <- 0 until newsTag.size()){
          val tag = newsTag.get(i).getKey
          val mappingList = editorTagMappingBr.value.get(tag)
          if (mappingList != null){
            val mappingTag = mappingList.get(0) //这里小类映射回大类是写死的用了get(0)，因为目前一个小类只会属于一个大类
            //如果不是情感类的新闻，就不入搜狗的三俗倒排
            if (mappingTag == "138")
              isLegalVulgarNews = true
          }
        }
      }
      if (isVulgarPic && newsSite != 1 && template != 4 )
        isLegalVulgarNews = true
      if (isVulgarViolence && template !=4)
        isLegalVulgarNews = true
      (newsid,sumReadCount,isLegalVulgarNews,isVulgarPic,isVulgarViolence,isVulgarSogou,status,createTime)
    } catch {
      case e : Exception => ("",-1,false,false,false,false,-1,0L)
    }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("lifengze:IndexCreatorVulgarNewsSG_test")
    sparkConf.set("spark.akka.frameSize","100")
    val sc = new SparkContext(sparkConf)
    val input = args(0)
    val partition = args(1)

    //获取低质新闻列表，在三俗倒排里把这些新闻过滤掉
    val cfBadNews = HBaseConfiguration.create()
    cfBadNews.set(TableInputFormat.INPUT_TABLE,"BD_REC:BadNews")
    HBaseUtilNew.setHBaseConfig(cfBadNews)
    val badNewsMap = sc.newAPIHadoopRDD(cfBadNews,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).map(x => x._2).map(x => (Bytes.toString(x.getRow),Bytes.toString(x.getValue(Bytes.toBytes("info"),Bytes.toBytes("ctr"))).toDouble)).filter(x => x._2 < 0.35).collectAsMap()
    //val badNewsMapBr = sc.broadcast(badNewsMap)




  }
}
*/
