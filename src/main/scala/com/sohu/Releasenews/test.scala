package com.sohu.Releasenews

import java.io.{File, FileWriter, PrintWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.io.Source
import scala.util.matching.Regex

/**

  */

object test{
  def main(args:Array[String]): Unit ={


    val f=new File("E:\\Data\\text\\releaseNews_local_sort\\Dic")
    val fw = new FileWriter(f,true)
    val writer = new PrintWriter(fw)


   val aaa= Source.fromFile("E:\\Data\\text\\releaseNews_local_sort\\InstitutionMedia_filter").getLines().flatMap(x=>{
      val attr=ArrayBuffer[String]()
      val title=x.split("\u0001")(0)
      val mediaName=x.split("\u0001")(1)
      val keyword=x.split("\u0001")(2)
      val regex = new Regex("[\u4e00-\u9fa5]")
      val content=(regex findAllIn title).mkString("")
      val title_cut=ToAnalysis.parse(content).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0)).mkString(" ")
      (title_cut+" "+mediaName+" "+keyword).split(" ").foreach(y=>attr.append(y))
      attr
    }).map(x=>(x,1)).toArray.groupBy(_._1).map(x=>(x._1,x._2.size)).toArray.sortBy(_._2).reverse.foreach(x=>writer.println(x))
    writer.flush()
    writer.close()


  }}
