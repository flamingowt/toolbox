package com.sohu.tool_box

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{Map => mMap}
import scala.util.matching.Regex

/**

  */

object extractFrom_mmbackup{
  def main(args:Array[String]): Unit ={


    val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
    val sc=new SparkContext(sparkconf)
    val rawData=sc.textFile("E:\\Data\\text\\news").filter(!_.contains("v1.1_video"))
        .map(x=>{
          val l = x.replace("\":\"", "@").replace("\",\"", "@")
          val contentP = new Regex("content@(.+?)@")
          val titleP = new Regex("title@(.+?)@")
          val content = (contentP findAllIn l).mkString(",").replace("content@", "").replace("@", "")
          val title = (titleP findAllIn l).mkString(",").replace("title@", "").replace("@", "")
          title+"\u0001"+content
        }).saveAsTextFile("E:\\Data\\text\\testnews")

}}

