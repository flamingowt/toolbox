/*
package com.sohu.news.longLabelNegativeFeedback

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linbai on 2017/5/19.
  */
object newsUserPvClk {
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

    val user_id_pvclk_file = args(0)
    val id_tags_file = args(1)
    val out = args(2)

    val user_id_pvclk = sc.textFile(user_id_pvclk_file).filter(x => x.split("@").length == 2 && x.split("\t").length == 2)
        .map(x => (x.split("@")(0),x.split("@")(1).split("\t")(0),x.split("@")(1).split("\t")(1)))

    val id_clk_pv = user_id_pvclk.map(x => (x._2,(x._3.toInt,1.0)))
        .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
        .map(x => (x._1,x._2._1,x._2._2))

    id_clk_pv.map(x => x._1 +"\t" + x._2 +"\t" + x._3).saveAsTextFile(out + "/id_clk_pv")

    val user_id_clk_pv = user_id_pvclk.map(x => (x._1,x._2,x._3.toInt,1.0))

    val id_clk_pv_br = sc.broadcast(id_clk_pv.map(x => (x._1,(x._2,x._3))).collectAsMap())

    val tag_id_clk_pv = sc.textFile(id_tags_file).filter(_.split("\t").length == 2)
        .map(x => (x.split("\t")(0),x.split("\t")(1).split(",").toSet.toArray))
        .flatMap(x => x._2.map(y => (y,x._1,id_clk_pv_br.value(x._1))))

    tag_id_clk_pv.map(x => x._1 + "\t" + x._2 + "\t" + x._3._1 + "\t" + x._3._2).saveAsTextFile(out + "/tag_id_clk_pv")

    val id_tags_br = sc.broadcast(tag_id_clk_pv.map(x => (x._2,Set(x._1))).reduceByKey(_|_).collectAsMap())
    val user_tag_clk_pv_ctr = user_id_clk_pv.map(x => (x._1,id_tags_br.value.getOrElse(x._2,Set("")).toArray.filter(_.length>0),x._3,x._4))
          .filter(_._2.length > 0)
          .flatMap(x => x._2.map(y =>(x._1 + "@" + y,(x._3,x._4))))
          .reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
          .map(x => (x._1,x._2._1,x._2._2,x._2._1/x._2._2.toDouble))
    user_tag_clk_pv_ctr.map(x => x._1 +"\t" +x._2 + "\t" +x._3 + "\t" +x._4).saveAsTextFile(out + "/user_tag_clk_pv_ctr")

    val tag_clk_pv_ctr = tag_id_clk_pv.map(x => (x._1,(x._3._1,x._3._2)))
        .reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
        .map(x => (x._1,x._2._1,x._2._2,x._2._1/x._2._2))

    tag_clk_pv_ctr.map(x => x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4).saveAsTextFile(out + "/tag_clk_pv_ctr")

    val id_users_br = sc.broadcast(user_id_pvclk.map(x => (x._2,Set(x._1))).reduceByKey(_|_).collectAsMap)

    val tag_users_num = tag_id_clk_pv.map(x => (x._1,id_users_br.value(x._2)))
        .reduceByKey(_|_).map(x => (x._1,x._2.size))

    tag_users_num.map(x => x._1 + "\t" + x._2.toString).saveAsTextFile(out + "/tag_users_num")

    val tag_users_num_br = sc.broadcast(tag_users_num.collectAsMap())
    val tag_user_clk_pv = tag_clk_pv_ctr.map(x => (x._1,x._2.toDouble/tag_users_num_br.value(x._1),x._3.toDouble/tag_users_num_br.value(x._1)))

    tag_user_clk_pv.map(x => x._1 + "\t" +x._2 +"\t" +x._3).saveAsTextFile(out +"/tag_converUser_clk_pv")
  }
}
*/
