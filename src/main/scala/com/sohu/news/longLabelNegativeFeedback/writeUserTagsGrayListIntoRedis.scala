package com.sohu.news.longLabelNegativeFeedback

import java.io.Serializable

//import com.sohu.adlog.push.storm.hbaseUtil.JedisClusterService
import com.sohu.news.common.dao.{NewsIdListDao, NewsRecDao}
import com.sohu.news.common.util.NewsRecDaoUtil
import com.sohu.news.common.po.ScoreMap
import org.apache.spark.{SparkConf, SparkContext}
import com.sohu.news.common.po.UserLongNegativeFeedBackPO
import com.sohu.news.hbaseUtil.JedisClusterService

import scala.collection.JavaConversions._

/**
  * Created by linbai on 2017/5/22.
  */
object writeUserTagsGrayListIntoRedis extends Serializable{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("bailin_writeUserTagsGrayListIntoRedis")

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

    val user_tag_pv_clk_smoothCtr_file = args(0)
    val tag_ral_ctr_file = args(1)
    val out = args(2)
	  val user_tag_clk_pv_ctr = args(3)
    val prefix = args(4)
	
	val user_tag_clk_pv = sc.broadcast(sc.textFile(user_tag_clk_pv_ctr)
		.map(x => {
			val items = x.split("\t")
			(items(0),(items(0).split("@")(1),"clk:" + items(1) + ",pv:" + items(2)))
		}).collectAsMap)
    val tag_ral_ctr = sc.textFile(tag_ral_ctr_file).map(x => (x.split("\t")(0),x.split("\t")(3).toDouble))
    val tag_ral_ctr_br = sc.broadcast(tag_ral_ctr.collectAsMap())

    val user_tags_score = sc.textFile(user_tag_pv_clk_smoothCtr_file)
        .map(x =>{
          val items = x.split("\t")
          val user = items(0)
          val tag = items(1)
          val smoothCtr = items(4).toDouble
          (user,tag,smoothCtr/tag_ral_ctr_br.value(tag))
        }).filter(_._3 < 1.0)
        .map(x => (x._1,Set((x._2,x._3))))
        .reduceByKey(_|_)
        .map(x =>x._1 + "\t" + x._2.toArray.map(y => y._1 +"@"+y._2).mkString(","))
		.repartition(10)
    user_tags_score.saveAsTextFile(out +"/user_tags_score")

    val label_version = "v1.0.0"
    user_tags_score.foreach(x =>{
      val newsRecDao: NewsRecDao = NewsRecDaoUtil.getInstance()
      val uln:UserLongNegativeFeedBackPO = new UserLongNegativeFeedBackPO()
      val m:java.util.Map[String,java.util.List[ScoreMap]] = new java.util.HashMap[String,java.util.List[ScoreMap]]()
      val user_id = x.split("\t")(0)
      uln.setUid(user_id)
      val tags_score = x.split("\t",2)(1).split(",")
      val orderIds = new java.util.ArrayList[ScoreMap]
      tags_score.foreach(y =>{
        val tag = y.split("@")(0)
        val score = (y.split("@")(1).toDouble*100000).toInt/100000.0
        orderIds.add(new ScoreMap(tag,score))
      })
      m.put(label_version,orderIds)
      uln.setTagNegative(m)
      newsRecDao.setUserLongNegativeFeedBack(uln)
    })

    val rst = user_tags_score.map(x =>{
      try {
        val uid = x.split("\t")(0)
        val newsRecDao: NewsRecDao = NewsRecDaoUtil.getInstance()
        val rr = newsRecDao.getUserLongNegativeFeedBack(uid).getTagNegative
		val longNeg = rr(label_version).toArray.mkString("@@@")
		uid + "\t" + longNeg
      } catch {
        case e: Exception => ""
      }
    }).filter(_.length > 0)
    rst.saveAsTextFile(out + "/redis")
	
	rst.foreachPartition(iter => {
		val jedisClusterService = new JedisClusterService("10.16.39.47:6396,10.16.39.47:6397,10.16.39.48:6396,10.16.39.48:6397,10.16.39.49:6396,10.16.39.49:6397,10.16.39.50:6396,10.16.39.50:6397,10.16.39.51:6396,10.16.39.51:6397,10.16.39.52:6396,10.16.39.52:6397,10.16.39.60:6396,10.16.39.60:6397,10.16.39.61:6396,10.16.39.61:6397,10.16.39.62:6396,10.16.39.62:6397,10.16.39.63:6396,10.16.39.63:6397,10.16.39.64:6396,10.16.39.64:6397,10.16.39.65:6396,10.16.39.65:6397")
		iter.foreach(x =>{
			val uid = x.split("\t")(0)
			val kv = x.split("\t")(1).split("@@@")
				.map(y => {
					val key = user_tag_clk_pv.value(uid+"@"+y.split(":")(0))
					key._1 + ":" + y.split(":")(1) + "@" + key._2
				}).mkString("#")
			jedisClusterService.set(prefix + uid,kv)
      jedisClusterService.expire(prefix + uid,60*60*24*90);
		})
	})
    val writeRedis = rst.mapPartitions(iter =>{ 
		val jedisClusterService = new JedisClusterService("10.16.39.47:6396,10.16.39.47:6397,10.16.39.48:6396,10.16.39.48:6397,10.16.39.49:6396,10.16.39.49:6397,10.16.39.50:6396,10.16.39.50:6397,10.16.39.51:6396,10.16.39.51:6397,10.16.39.52:6396,10.16.39.52:6397,10.16.39.60:6396,10.16.39.60:6397,10.16.39.61:6396,10.16.39.61:6397,10.16.39.62:6396,10.16.39.62:6397,10.16.39.63:6396,10.16.39.63:6397,10.16.39.64:6396,10.16.39.64:6397,10.16.39.65:6396,10.16.39.65:6397")
		iter.map(x =>{
			val uid = prefix + x.split("\t")(0)
			jedisClusterService.get(uid)
		})
	})
	writeRedis.saveAsTextFile(out + "/writeRedis")
  }
}