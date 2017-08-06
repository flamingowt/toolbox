package com.sohu.Static

import java.text.DecimalFormat

import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object label_label {
  def main(args:Array[String]): Unit = {


    val conf = new SparkConf().setAppName("wangteng_AffinityPropagationSuite")
    conf.set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val label=argsMap.getOrElse("--label", "")
    val out = argsMap.getOrElse("--output", "")
    val label_br=sc.broadcast(sc.textFile(label).map(x=>(x.split(",")(0),x.split(",")(1))).collect())
    sc.textFile(input).filter(_.split("\u0001",2).length>1).flatMap(x=>{
      val label_content=ArrayBuffer[(String,String)]()
      val sp=x.split("\u0001",2)
      val label_merge=sp(0).split("&").map(_.split("-")(0).split("_")(0)).distinct
      label_br.value.foreach(y=>{
        if(label_merge.contains(y._1) && label_merge.contains(y._2)){
          label_content.append((y._1+","+y._2,x))
        }
      })
      label_content
    }).groupByKey().map(x=>{
      val num=x._2.size
      var Label_content=""
  /*    if(num>101){
        Label_content=x._2.take(100).map(y=>x._1+"-"+num+"\u0001"+y).mkString("\n")
      }
      else */Label_content=x._2.map(y=>x._1+"-"+num+"\u0001"+y).mkString("\n")
      Label_content
    }).repartition(1).saveAsTextFile(out)
  }

}


