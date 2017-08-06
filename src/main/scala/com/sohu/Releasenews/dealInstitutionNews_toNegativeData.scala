package com.sohu.Releasenews

import java.io.{File, FileWriter, PrintWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.io.Source
import scala.util.matching.Regex

/**

  */

object dealInstitutionNews_toNegativeData{
  def main(args:Array[String]): Unit ={


    val f=new File("E:\\Data\\text\\releaseNews_local_sort\\negativeData\\negativedata")
    val fw = new FileWriter(f,true)
    val writer = new PrintWriter(fw)


   val aaa= Source.fromFile("E:\\Data\\text\\negativeData_raw").getLines().filter(_.split("\u0001").length>2).map(x=>{

      val label=x.split("\u0001")(0)
      val title=x.split("\u0001")(1)
      val content=x.split("\u0001")(2)
      val regex = new Regex("[\u4e00-\u9fa5]")
      val title_content=(regex findAllIn title+content).mkString("")
      val title_content_cut=ToAnalysis.parse(title_content).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0)).mkString(" ")
     "-1"+"\u0001"+title_content_cut
    }).take(30967).foreach(x=>writer.println(x))
    writer.flush()
    writer.close()


  }}
