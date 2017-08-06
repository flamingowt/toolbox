package com.sohu.news.longLabelNegativeFeedback

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linbai on 2017/5/22.
  */
object userLabelGrapList {
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

    val user_tag_pv_clk_ctr_file = args(0)
    val tag_converUser_clk_pv_file = args(1)
    val tag_clk_pv_ctr_file = args(2)

    val conver_threshold = args(3).toInt
    val pv_threahold = args(4).toInt

    val out = args(5)

    var threshold = 30.0

    if(args.length >= 7) threshold = args(6).toDouble

    val user_tag_pv_clk_ctr = sc.textFile(user_tag_pv_clk_ctr_file).map(x => (x.split("[@\t]")(0), x.split("[@\t]")(1), x.split("[@\t]")(3).toDouble, x.split("[@\t]")(2).toDouble, x.split("[@\t]")(4).toDouble))

    val tag_converUser_clk_pv = sc.textFile(tag_converUser_clk_pv_file)
        .map(x => (x.split("\t")(0), x.split("\t")(1), x.split("\t")(2)))
        .filter(_._3.toDouble > conver_threshold)
        .map(x => (x._1,(x._2.toDouble/threshold,x._3.toDouble/threshold)))

    val tag_clk_pv_ctr =  sc.textFile(tag_clk_pv_ctr_file)
        .map(x => (x.split("\t")(0), x.split("\t")(1), x.split("\t")(2), x.split("\t")(3)))
        .filter(_._3.toDouble > pv_threahold)
        .map(x => (x._1,(x._3.toDouble,x._4.toDouble)))

    val tags = tag_converUser_clk_pv.join(tag_clk_pv_ctr).map(x => (x._1,(x._2._1._1,x._2._1._2)))
    val tags_alpha_beta = sc.broadcast(tags.collectAsMap)

    val user_tag_pv_clk_smoothCtr = user_tag_pv_clk_ctr.filter(x => tags_alpha_beta.value.contains(x._2)).map(x => (x._1,tags_alpha_beta.value(x._2)._1 +x._4,tags_alpha_beta.value(x._2)._2 + x._3,x._5,x._2))
    user_tag_pv_clk_smoothCtr.map(x => x._1 + "\t" + x._5 + "\t" + x._2 + "\t" + x._3 + "\t" +x._2/x._3 ).saveAsTextFile(out)
  }
}