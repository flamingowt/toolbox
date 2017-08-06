package com.sohu.Static

import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object toutiao_pv_click {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wangteng_AffinityPropagationSuite")
    conf.set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val out = argsMap.getOrElse("--output", "")
   val raw= sc.textFile(input).cache()

    val click=raw.filter(_.split("\t",2)(0).equals("1"))
    val pv=raw.filter(_.split("\t",2)(0).equals("0"))

    click.filter(_.split("\t").length==3).flatMap(x=>{
      val label_attr=ArrayBuffer[(String,Int)]()
      val label= x.split("\t")(2).split(",").foreach(y=>label_attr.append((y,1)))
      label_attr
    }).reduceByKey((x,y)=>x+y).sortBy(_._2,false).saveAsTextFile(out+"/click")

    pv.filter(_.split("\t").length==3).flatMap(x=>{
      val label_attr=ArrayBuffer[(String,Int)]()
      val label= x.split("\t")(2).split(",").foreach(y=>label_attr.append((y,1)))
      label_attr
    }).reduceByKey((x,y)=>x+y).sortBy(_._2,false).saveAsTextFile(out+"/pv")

  }

}


