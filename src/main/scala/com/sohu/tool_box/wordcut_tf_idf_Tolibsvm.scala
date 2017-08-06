package com.sohu.tool_box

import java.io.{FileWriter, PrintWriter}
import java.text.DecimalFormat

import scala.collection.mutable.{ArrayBuffer, Map => mMap}
import scala.io.Source

/**

  */

object wordcut_tf_idf_Tolibsvm{//将分好词的文档集（每行是一篇文章）按tfidf特征转换成libsvm的输入格式：类别 第一维特征编号：第一维特征值 第二维特征编号：第二维特征值…
  def main(args:Array[String]): Unit ={
    val df: DecimalFormat = new DecimalFormat("#.###")
    val path_vec="E:\\Data\\text\\alldata"
    val path_test="E:\\Data\\text\\testnews\\testnews_cutword"
    val filenum=Source.fromFile(path_vec).getLines().length
//计算训练集向量
    val model_idf =Source.fromFile(path_vec).getLines().flatMap(x=>{
      val line=ArrayBuffer[(String,Int)]()
      x.split(" ").distinct.foreach(y=>line.append((y,1)))
      line
    }).toArray.groupBy(_._1).map(x=>(x._1,x._2.length)).toMap

   val word_tfIdf =Source.fromFile(path_vec).getLines().map(x=>{
     val word_tf_idf = x.split(" ").map(word=>(word,1)).groupBy(_._1).map(y=>(y._1,y._2.length,Math.log(filenum.toDouble/model_idf(y._1).toDouble)))
     val word_tfidf = word_tf_idf.map(y=>(y._1,y._2 * y._3)).toArray.sortBy(_._2).reverse.take(15).toMap
     word_tfidf
   })
   val vec= word_tfIdf.flatMap(x=>{
      val word=ArrayBuffer[String]()
      x.foreach(w=>word.append(w._1))
      word
    }).toArray.distinct.filter(_.length>1)//.foreach(x=>println(x))
    val vecIndex = vec.zipWithIndex.toMap

    //将测试集映射为向量
    val testset_idf =Source.fromFile(path_test).getLines().flatMap(x=>{
      val line=ArrayBuffer[(String,Int)]()
      x.split(" ").distinct.foreach(y=>line.append((y,1)))
      line
    }).toArray.groupBy(_._1).map(x=>(x._1,x._2.length)).toMap

    val word_tfIdf_Neg =Source.fromFile(path_test).getLines().map(x=>{
      val word_tf_idf = x.split(" ").map(word=>(word,1)).groupBy(_._1).map(y=>(y._1,y._2.length,Math.log(filenum.toDouble/testset_idf(y._1).toDouble)))
      val word_tfidf = word_tf_idf.map(y=>(y._1,y._2 * y._3)).toMap
      word_tfidf
    })

    val fw_N = new FileWriter("E:\\Data\\text\\testnews\\testnews_format",true)
    val writer_N = new PrintWriter(fw_N)

    word_tfIdf_Neg.foreach(line=>{
      val DocVec =ArrayBuffer[(Int,String)]()//num,tfidf
      line.foreach(x=>{
        if(vecIndex.contains(x._1))
          DocVec.append((vecIndex(x._1),df.format(x._2)))
      })
      writer_N.println("-1 "+DocVec.toArray.sortBy(_._1).map(x=>x._1+":"+x._2).mkString(" "))

    })

    writer_N.flush()
    writer_N.close()


  }}