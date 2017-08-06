package com.sohu.Static

import java.io.ByteArrayInputStream

import com.sohu.avro.dao.Event
import com.sohu.proto.rawlog.Log

import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object PageSource {
  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wangteng_AffinityPropagationSuite")
    conf.set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val out = argsMap.getOrElse("--output", "")


    val data=sc.textFile(input).filter(_.split("\t").length==3).map(x=>{
     val sp= x.split("\t")
      val uid=sp(0).split("@")(0)
      val eventid=sp(1)
      val paesource=sp(2)
      (uid,eventid,paesource)
    }).cache()
    val click=data.filter(x=>x._2.equals("1")&& !x._1.equals("5b9a1293-2554-3199-a215-026e2a1a5aa8"))
    //val pv=data.filter(_._1==0)

    click.map(x=>(x._3,1)).reduceByKey((x,y)=>x+y).repartition(1).saveAsTextFile(out)
  }

}


