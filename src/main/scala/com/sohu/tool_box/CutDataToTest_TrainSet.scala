package com.sohu.tool_box

/**
  * Created by Administrator on 2017/2/8.
  */

import java.io.{FileWriter, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CutDataToTest_TrainSet{//将数据集按2:8切分为训练集合测试集

    def main(args:Array[String]):Unit = {
        val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
        val sc=new SparkContext(sparkconf)

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
      val neg= sc.textFile("E:\\Data\\text\\ztemptask\\neg_tfidf_libsvmInput")
      val pos= sc.textFile("E:\\Data\\text\\ztemptask\\pos_tfidf_libsvmInput")
      var data=neg.union(pos).randomSplit(Array(2,8))
      val fw1 = new FileWriter("E:\\Data\\text\\ztemptask\\test",true)
      val fw2 = new FileWriter("E:\\Data\\text\\ztemptask\\train",true)

      val writer1 = new PrintWriter(fw1)
      val writer2 = new PrintWriter(fw2)
      data(0).collect().foreach(x=> writer1.println(x))//.saveAsTextFile("E:\\Data\\text\\trainSet\\test")
      data(1).collect().foreach(x=> writer2.println(x))//.saveAsTextFile("E:\\Data\\text\\trainSet\\train")
      writer1.flush()
      writer1.close()
      writer2.flush()
      writer2.close()



    }

  }


