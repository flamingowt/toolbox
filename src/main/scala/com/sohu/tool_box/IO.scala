package com.sohu.tool_box

/**
  * Created by Administrator on 2017/2/8.
  */

import java.io.{File, FileWriter, PrintWriter}

object IO {

    def main(args:Array[String]):Unit = {
      for(d <- subDir(new File("E:\\wt\\right")))
        println(d.getName)

    }
    def subDir(dir: File): Iterator[File] = {
      val d = dir.listFiles.filter(_.isDirectory)
      val f = dir.listFiles.toIterator
      f ++ d.toIterator.flatMap(subDir _)
    }
  val fw = new FileWriter("E:\\wangteng\\tag_userNum",true)
  val writer = new PrintWriter(fw)
  writer.println("")
  writer.flush()
  writer.close()

  }


