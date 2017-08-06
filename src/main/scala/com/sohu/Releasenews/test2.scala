package com.sohu.Releasenews

import java.io.{File, FileWriter, PrintWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.io.Source
import scala.util.matching.Regex

/**

  */

object test2{
  def main(args:Array[String]): Unit ={

val wordFilter=Source.fromFile("E:\\LyricProject\\war\\war_cut_n").getLines().flatMap(x=>{
  val attr =ArrayBuffer[(String,Int)]()
  x.split(" ").foreach(w=>attr.append((w,1)))
  attr
}).toArray.groupBy(_._1).map(x=>(x._1,x._2.length)).toArray.sortBy(_._2).reverse.take(1500).map(_._1)

    val f=new File("E:\\LyricProject\\war\\cluster_word_30")
    val fw = new FileWriter(f,true)
    val writer = new PrintWriter(fw)


   val word= Source.fromFile("E:\\LyricProject\\war\\war_vectors.txt").getLines().map(_.split("\t")(0))
   val cluster= Source.fromFile("E:\\LyricProject\\war\\labels_30").getLines()
   val aaa= cluster.zip(word).toArray.groupBy(_._1).toArray.sortBy(_._1.toInt).map(_._2).foreach(x=>{
    val line=x.map(_._2).intersect(wordFilter)
     writer.println(line.mkString(" ")+"\n")
   })
    writer.flush()
    writer.close()


  }}
