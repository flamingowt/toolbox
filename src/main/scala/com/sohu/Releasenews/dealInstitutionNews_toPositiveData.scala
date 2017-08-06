package com.sohu.Releasenews

import java.io.{File, FileWriter, PrintWriter}

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.io.Source
import scala.util.matching.Regex

/**

  */

object dealInstitutionNews_toPositiveData{
  def main(args:Array[String]): Unit ={
    val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
    val sc=new SparkContext(sparkconf)

/*    val f=new File("E:\\Data\\text\\releaseNews_local_sort\\Dic")
    val fw = new FileWriter(f,true)
    val writer = new PrintWriter(fw)*/

val intitutionMedianews_content=sc.textFile("E:\\Data\\text\\releaseNews_local_sort\\InstitutionMedia").map(x=>(x.split("\u0001")(0),x.split("\u0001")(4))).distinct.cache()

   val intitutionMedianews= sc.textFile("E:\\Data\\text\\releaseNews_local_sort\\InstitutionMedia_filter").map(x=>{
      val attr=ArrayBuffer[String]()
      val title=x.split("\u0001")(0)
      val mediaName=x.split("\u0001")(1)
      val keyword=x.split("\u0001")(2)
      val regex = new Regex("[\u4e00-\u9fa5]")
      val titlefilted=(regex findAllIn title).mkString("")
      val title_cut=ToAnalysis.parse(titlefilted).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0)).mkString(" ")
      (title_cut+" "+mediaName+" "+keyword).split(" ").foreach(y=>attr.append(y))
     (title,attr.mkString(" "))
    }).join(intitutionMedianews_content).map(x=>{
     val regex = new Regex("[\u4e00-\u9fa5]")
     val content=(regex findAllIn x._2._2).mkString("")
     val content_cut=ToAnalysis.parse(content).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0))
     val titleKeyword=x._2._1.split(" ")
     val beishu=(content_cut.size.toDouble/titleKeyword.size.toDouble).toInt
     titleKeyword.map(w=>{
       var word=w
       for(i<-1 to beishu){
         word=word+" "+w
       }
       word
     })
     "1"+"\u0001"+titleKeyword.mkString(" ")+" "+content_cut.mkString(" ")
   }).repartition(1).saveAsTextFile("E:\\Data\\text\\releaseNews_local_sort\\positiveData")
   /* writer.flush()
    writer.close()*/


  }}
