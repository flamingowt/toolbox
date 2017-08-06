package com.sohu.Releasenews

/**
  * Created by Administrator on 2017/2/8.
  */

import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object flatmapFromHbaseJoinWithTextV1_1{//将readfromHbase里读出来的数据处理成tag title content形式

    def main(args:Array[String]):Unit = {
      val sparkConf = new SparkConf().setAppName("wangteng_readfromHbase")
      sparkConf.set("spark.akka.frameSize","100")
      val sc = new SparkContext(sparkConf)
      val argsMap = ParseArgs.parse(args)
      val input = argsMap.getOrElse("--input", "")
      val out = argsMap.getOrElse("--output", "")
      val tagDic_path = argsMap.getOrElse("--tagDic_path", "")

     val tagDic_br=sc.broadcast(sc.textFile(tagDic_path).filter(_.split("\t").length>1).map(x=>(x.split("\t")(1),x.split("\t")(0))).collectAsMap())
      val label_news=sc.textFile(input).filter(_.split("\u0001").length>3).flatMap(x=>{
        val sp=x.split("\u0001")
        val title=sp(1)
        val tags=sp(2)
        val content=sp(3)
        val attr=ArrayBuffer[(String,String)]()
       val labels= tags.split("_").filter(!_.equals("v1.1")).filter(!_.equals("text")).map(y=>{
          var label=""
          if(tagDic_br.value.contains(y)){
            label=tagDic_br.value(y)
          }
           label.split("-")(0)//只算大类
        }).foreach(y=>attr.append((y,title+"\u0001"+content)))
          attr
      }).groupByKey()
          .filter(!_._1.equals("情感")).filter(!_._1.equals("美文")).filter(!_._1.equals("风水")).filter(!_._1.equals("佛教")).filter(!_._1.equals("")).filter(!_._1.equals("彩票")).filter(!_._1.equals("搞笑")).filter(!_._1.equals("故事")).filter(!_._1.equals("育儿")).filter(!_._1.equals("历史"))
        .flatMap(x=>{
          val attr =ArrayBuffer[String]()
             x._2.foreach(y=>attr.append(x._1+"\u0001"+y))
          attr
      }).distinct().repartition(1).saveAsTextFile(out)




    }

  }


