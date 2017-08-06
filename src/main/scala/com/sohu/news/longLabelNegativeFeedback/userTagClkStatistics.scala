package com.sohu.news.longLabelNegativeFeedback

/**
  * Created by linbai on 2017/5/18.
  */

import java.text.DecimalFormat
import org.apache.spark.{SparkConf, SparkContext}

object userTagClkStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("recExpand")
      .set("spark.akka.frameSize", "2000")
      .set("spark.driver.maxResultSize", "36g")
      .set("spark.rdd.compress", "true")
      .set("spark.core.connection.ack.wait.timeout", "6000")
      .set("spark.akka.retry.wait", "90000")
      .set("spark.akka.timeout", "90000")
      .set("spark.storage.blockManagerHeartBeatMs", "90000")
      .set("spark.storage.blockManagerTimeoutIntervalMs", "1200000")
      .set("spark.storage.blockManagerSlaveTimeoutMs", "3000000")
      .set("spark.worker.timeout", "600")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.shuffle.spill", "false")
      .set("spark.yarn.executor.memoryOverhead", "4000")
      .set("spark.yarn.driver.memoryOverhead", "4000")

    val sc = new SparkContext(conf)

    val userTagFile = args(0)
    val out = args(1)

    val df: DecimalFormat = new DecimalFormat("#.####")

    val userTag = sc.textFile(userTagFile).filter(x => x.split("@").length == 2 && x.split("@")(1).split("\t").length == 3).repartition(100)
        .map(x => (x.split("@")(0),x.split("@")(1).split("\t")(0),x.split("\t")(1).toDouble,x.split("\t")(2).toDouble))

    val userTag_clk_pv = userTag.map(x => (x._1 + "\t" + x._2,(x._3,x._4))).reduceByKey((x,y) =>(x._1+y._1,x._2+y._2))
    val user_tag_ctr = userTag_clk_pv.map(x => (x._1,x._2._2,df.format(x._2._1/x._2._2)))
    user_tag_ctr.map(x => x._1 + "\t" + x._2 + "\t" + x._3).saveAsTextFile(out + "/user_tag_pv_ctr")

    val tag_pv_ctr = userTag.map(x => (x._2,(x._3,x._4))).reduceByKey((x,y) => (x._1+y._1,x._2+y._2)).map(x => x._1 + "\t" + x._2._2 +"\t" + df.format(x._2._1/x._2._2))
    tag_pv_ctr.saveAsTextFile(out + "/tag_pv_ctr")

    val tag_ctr_br = sc.broadcast(tag_pv_ctr.map(x => (x.split("\t")(0),x.split("\t")(2))).collectAsMap)

    val user_tags_ctr = user_tag_ctr.map(x => (x._1.split("\t")(0),(Set(x._1.split("\t")(1) +"@" + tag_ctr_br.value(x._1.split("\t")(1))), x._2,x._3)))
            .reduceByKey((x,y) => (x._1 | y._1,x._2,x._3))
            .map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._3  + "\t"+ x._2._1.mkString(","))
    user_tags_ctr.saveAsTextFile(out + "/user_tags_ctr")
  }
}
