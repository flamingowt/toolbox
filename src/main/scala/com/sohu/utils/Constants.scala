package com.sohu.util

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by xiaojia on 2016/3/9.
  */
object Constants {
  //val HBASE_TABLE=Bytes.toBytes("BD_WH:REAL_TIME_DATA")
  val HBASE_TABLE=Bytes.toBytes("BD_WH:REAL_TIME_CLICK_DATA")
  val SNAP_PREFIX="SNAP_"
  val HOUR_PREFIX="H_"
  val DAY_PREFIX="D_"

  val USERTAG_HBASE_KEY = "k"
  val USERTAG_TABLE = "BD_REC:usertag"
  val USERTAG_FAMILY_REDIS = "tag_redis"
  val USERTAG_FAMILY_HBASE = "tag_hbase"

  val NEWSTAG_HBASE_KEY = "k"
  val NEWSTAG_TABLE = "BD_REC:newstag"
  val NEWSTAG_FAMILY_REDIS = "tag_redis"
  val NEWSTAG_FAMILY_HBASE = "tag_hbase"

  val TAG_SPLIT_FORMAT_LEVEL1 = ","
  val TAG_SPLIT_FORMAT_LEVEL2 = ":"

  val REDIS_TAG_LENGTH = 10
  val myredisserver = "10.16.39.47:6396,10.16.39.47:6397,10.16.39.48:6396,10.16.39.48:6397,10.16.39.49:6396,10.16.39.49:6397,10.16.39.50:6396,10.16.39.50:6397,10.16.39.51:6396,10.16.39.51:6397,10.16.39.52:6396,10.16.39.52:6397,10.16.39.60:6396,10.16.39.60:6397,10.16.39.61:6396,10.16.39.61:6397,10.16.39.62:6396,10.16.39.62:6397,10.16.39.63:6396,10.16.39.63:6397,10.16.39.64:6396,10.16.39.64:6397,10.16.39.65:6396,10.16.39.65:6397"
  val jedisClusterTimeout = 5000
  val maxRedirections = 1000
  val jedisExceptionSleep: Int = 1000
  val redisAliveKey: String = "redis_alive"

  val NEWSREC_EVENT_CLICK = 3
  val NEWSREC_EVENT_PV = 4

  // todo 修改正式环境
  //   val PRODUCTION_ENV = false
  val PRODUCTION_ENV = true

  //   def main(args: Array[String]): Unit = {
  //      val categoryMap = CATEGORYMAP.split(",").map(x => (x.split(":")(0), x.split(":")(1))).toMap
  //      println(categoryMap)
  //      println(categoryMap.get("text_498").get.toString)
  //   }
}
