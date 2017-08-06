package com.sohu.Releasenews

import java.io.{FileWriter, PrintWriter}

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
import scala.io.Source


object InstitutionMediaNews_filter {

  def main(args: Array[String]) {
  //交通、驾、预警、竟然、案例、抓拍、晒、知识、支招、常识、真相、揭秘、解密、你们、这些、错过、陷阱
    val fw_N = new FileWriter("E:\\Data\\text\\releaseNews_local_sort\\InstitutionMedia_filter",true)
    val writer_N = new PrintWriter(fw_N)
    val aaaaaaa=Source.fromFile("E:\\Data\\text\\releaseNews_local_sort\\InstitutionMedia").getLines().filter(_.split("\u0001").length>5)
      .filter(!_.split("\u0001")(0).contains("预警")).filter(!_.split("\u0001")(0).contains("驾")).filter(!_.split("\u0001")(0).contains("交通"))
      .filter(!_.split("\u0001")(0).contains("厨房")).filter(!_.split("\u0001")(0).contains("案例")).filter(!_.split("\u0001")(0).contains("抓拍"))
      .filter(!_.split("\u0001")(0).contains("竟然")).filter(!_.split("\u0001")(0).contains("晒")).filter(!_.split("\u0001")(0).contains("知识"))
      .filter(!_.split("\u0001")(0).contains("支招")).filter(!_.split("\u0001")(0).contains("识房")).filter(!_.split("\u0001")(0).contains("真相"))
      .filter(!_.split("\u0001")(0).contains("解密")).filter(!_.split("\u0001")(0).contains("揭秘")).filter(!_.split("\u0001")(0).contains("你们"))
      .filter(!_.split("\u0001")(0).contains("这些")).filter(!_.split("\u0001")(0).contains("错过")).filter(!_.split("\u0001")(0).contains("陷阱"))
      .filter(!_.split("\u0001")(1).contains("吃"))
      .filter(!_.split("\u0001")(1).contains("娱")).filter(!_.split("\u0001")(1).contains("媒")).filter(!_.split("\u0001")(1).contains("科技"))
      .filter(!_.split("\u0001")(1).contains("装修"))
      .map(x=>{
        val sp=x.split("\u0001")
        (sp(3),sp(0),sp(0)+"\u0001"+sp(1)+"\u0001"+sp(2)+"\u0001"+sp(3))
      }).filter(!_._1.contains("娱乐")).filter(!_._1.contains("情感")).filter(!_._1.contains("健康")).filter(!_._1.contains("车"))
      .filter(_._1.contains("社会")).filter(_._1.contains("政策"))
      .map(x=>(x._2,x._3)).toArray.groupBy(_._1).map(_._2.take(1).mkString("")).toArray.map(_.split(",",2)(1).replace(")","")).filter(_.split("\u0001").length>2).foreach(x=>writer_N.println(x))
  }
}