package com.sohu.Releasenews

import java.io.{FileWriter, PrintWriter}
import java.text.DecimalFormat

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.util.matching.Regex

/**

  */

object wordcut_tfidf_toLibSvm_spark{
  def main(args:Array[String]): Unit ={
    val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
    val sc=new SparkContext(sparkconf)
    val DicvecIndex_br=sc.broadcast(sc.textFile("E:\\Data\\text\\ztemptask\\DicvecIndex").map(x=>(x.split("\u0001")(0),x.split("\u0001")(1))).collectAsMap())


    def cutword(str:String):String={
      val regex = new Regex("[\u4e00-\u9fa5]")
      val content=(regex findAllIn str).mkString("")
      val line=ToAnalysis.parse(content).toString().split(",").filter(_.split("/").length>1).map(x=>x.split("/")(0)).mkString(" ")
      line
    }
//将训练集映射为向量
    val dataSet_cut=sc.textFile("E:\\Data\\text\\ztemptask\\trainSet_cut").cache()

    val trainSet_cut=sc.textFile("E:\\Data\\text\\ztemptask\\trainSet_cut").cache()
    val trainset_idf =trainSet_cut.map(x=>x.replace(x.split("\u0001")(0),"").trim).flatMap(x=>{
      val line=ArrayBuffer[(String,Int)]()
      x.split(" ").distinct.foreach(y=>line.append((y,1)))
      line
    }).groupBy(_._1).map(x=>(x._1,x._2.size)).collectAsMap()

    val trainset_idf_br=sc.broadcast(trainset_idf)
    val filenum_br=sc.broadcast(trainSet_cut.count())

    val word_tfIdf =dataSet_cut.map(x=>{
      val sp=x.split("\u0001")
      var content=""
      if(x.split("\u0001").length>1){
        content=sp(1)
      }
      val word_tf_idf = sp(1).split(" ").map(word=>(word,1)).groupBy(_._1).map(y=>{
        if (trainset_idf_br.value.contains(y._1)){
          (y._1,y._2.length,Math.log(filenum_br.value.toDouble/trainset_idf_br.value(y._1).toDouble))
        }
        else {(y._1, y._2.length,-1.toDouble)}
      }).filter(_._3!= -1)
      val word_tfidf = word_tf_idf.map(y=>(y._1,y._2 * y._3)).toMap
      (sp(0),word_tfidf)
    })
    val df: DecimalFormat = new DecimalFormat("#.###")
    val fw = new FileWriter("E:\\Data\\text\\ztemptask\\train_libsvmInput",true)
    val writer = new PrintWriter(fw)

    word_tfIdf.collect().foreach(line=>{
      val DocVec =ArrayBuffer[(Int,String)]()
      line._2.foreach(x=>{
        if(DicvecIndex_br.value.contains(x._1))
          DocVec.append((DicvecIndex_br.value(x._1).toInt,df.format(x._2)))
      })
      writer.println(line._1+" "+DocVec.toArray.sortBy(_._1).map(x=>x._1+":"+x._2).mkString(" "))
    })

    writer.flush()
    writer.close()

  }}
