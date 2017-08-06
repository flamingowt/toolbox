package com.sohu.Static

import java.text.DecimalFormat

import com.sohu.news.common.parse.NewsInfoParse
import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object extractFrom_mmbackup {
  def main(args:Array[String]): Unit = {


    val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
    val sc=new SparkContext(sparkconf)


    val rawData=sc.textFile("E:\\Data\\text\\news").mapPartitions(x=>{
      x.map(news=>{
        val parser=NewsInfoParse.parseNewsInfo(news)
        var line=""
        try{
          val content = parser.getContent
          val title = parser.getTitle
          val newsid = parser.getArticleId
          line=title+"@"+newsid+"\u0001"+content
        }
           line
      }).filter(!_.equals(""))
    }).foreach(x=>println(x))
  }

}


