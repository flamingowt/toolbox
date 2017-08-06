package com.sohu.utils.HBaseUtil

import scala.collection.mutable.Map

/**
 * Created by chaoshao on 2016/7/6.
 */
object ParseArgs {
  def parse(args: Array[String]): Map[String,String] ={
    val argsMap =Map[String,String]()
    val rangeIndex = new Range(0,args.length,2)
    for(i<-rangeIndex){
      argsMap+=(args(i)->args(i+1))
    }
    argsMap
  }
}
