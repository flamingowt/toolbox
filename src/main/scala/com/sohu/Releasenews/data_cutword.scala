package com.sohu.Releasenews

import java.io.{File, FileWriter, PrintWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.mutable.{Map => mMap}
import scala.io.Source
import scala.util.matching.Regex

/**

  */

object data_cutword{
  def main(args:Array[String]): Unit ={


    val f=new File("E:\\Data\\text\\ztemptask\\extendNews\\news_cut")
    val fw = new FileWriter(f,true)
    val writer = new PrintWriter(fw)

    val f1=new File("E:\\Data\\text\\ztemptask\\extendNews\\news_label")
    val fw1 = new FileWriter(f1,true)
    val writer1 = new PrintWriter(fw1)

    Source.fromFile("E:\\Data\\text\\ztemptask\\extendNews\\extendnews").getLines().foreach(x=>{
      val label=x.split("\u0001")(0)
      val title_content=x.split("\u0001",2)(1)
      val regex = new Regex("[\u4e00-\u9fa5]")
      val content=(regex findAllIn title_content).mkString("")
      val line=ToAnalysis.parse(content).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0)).mkString(" ")
      if(!label.equals("") && !line.equals("")) {
        writer.println(label)
        writer1.println(line)
      }
    })
    writer.flush()
    writer.close()
    writer1.flush()
    writer1.close()

  }}
