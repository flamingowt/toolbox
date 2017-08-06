package com.sohu.Static

import java.text.DecimalFormat

import com.sohu.utils.HBaseUtil.ParseArgs
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


object temptask {
  def main(args:Array[String]): Unit = {

  /*  val conf = new SparkConf().setAppName("wangteng_AffinityPropagationSuite")
    conf.set("spark.akka.frameSize", "2047")
    val sc = new SparkContext(conf)
    val argsMap = ParseArgs.parse(args)
    val input = argsMap.getOrElse("--input", "")
    val input2 = argsMap.getOrElse("--input2", "")
    val out = argsMap.getOrElse("--output", "")*/

  val sparkconf=new SparkConf().setMaster("local").setAppName("forTest")
    val sc=new SparkContext(sparkconf)
    val input="D:\\wateng\\src\\target\\user_id_pv_clk"
      val input2="D:\\wateng\\src\\target\\news_labels"
        val out="D:\\wateng\\src\\target\\result"
   /* val user_id_pv_clk = sc.textFile(input).filter(_.split("\t").length == 4).flatMap(x =>{
      val attr=ArrayBuffer[String]()
      val pv_clk = x.split("\t")(0)
      val user = x.split("\t")(1)
      val pagesourceNum=x.split("\t")(3)
      val ids = x.split("\t")(2).split(",").foreach(y => attr.append(user + "@" + y + "\t" + pv_clk + "\t"+pagesourceNum))
      attr
    })saveAsTextFile(out + "/user_id_pv_clk_pagesourceNum2")*/

   val df: DecimalFormat = new DecimalFormat("#.####")
    val newsid_label_br=sc.broadcast(sc.textFile(input2).filter(_.split("\t").length == 2).map(x=>(x.split("\t")(0),x.split("\t")(1))).collectAsMap())

    val user_id_pv_clk = sc.textFile(input).filter(_.split("\t").length == 2).flatMap(x=>{
      val labels_pvclick=ArrayBuffer[(String,String)]()
      val sp=x.split("\t")
      val pv_click=sp(1)
      val newsid=sp(0).split("@")(1)

      if(newsid_label_br.value.contains(newsid))
      newsid_label_br.value(newsid).split(",").foreach(y=>labels_pvclick.append((y,pv_click)))


      labels_pvclick
    }).cache()


    print(user_id_pv_clk)
   /* user_id_pv_clk.groupByKey().map(x=>{
     val pv= x._2.filter(_.equals("0")).size
      val click= x._2.filter(_.equals("1")).size
      (pv,x._1+"\t"+pv+"\t"+df.format(click/pv))
    }).repartition(1).saveAsTextFile(out)

*/
    sc.textFile("xiaonansong/newsAPP/newsLabeling/toutiao_category/{train,test}").filter(_.split("\u0001").length>3).filter(x=>x.split("\u0001")(1).contains("发布")||x.split("\u0001")(1).contains("颁布")||x.split("\u0001")(1).contains("发行")||x.split("\u0001")(1).contains("宣传")||x.split("\u0001")(1).contains("宣布")).filter(!_.split("\u0001")(1).contains("现身")).filter(!_.split("\u0001")(1).contains("出席")).filter(!_.split("\u0001")(0).contains("娱乐-八卦")).filter(!_.split("\u0001")(0).contains("娱乐-低俗")).map(x=>{x.split("\u0001")(0)+"\u0001"+x.split("\u0001")(1)+"\u0001"+x.split("\u0001")(2)}).repartition(1).saveAsTextFile("wangteng/newsRelease/positiveData")
    sc.textFile("xiaonansong/newsAPP/newsLabeling/toutiao_category/{train,test}").filter(_.split("\u0001").length>3).map(x=>{x.split("\u0001")(1)+"\u0001"+x.split("\u0001")(2)}).saveAsTextFile("wangteng/newsRelease/wholeData")
    sc.textFile("xiaonansong/newsAPP/newsLabeling/toutiao_category/{train,test}").filter(_.split("\u0001").length>3).filter(!_.split("\u0001")(1).contains("发布")).filter(!_.split("\u0001")(1).contains("颁布")).filter(!_.split("\u0001")(1).contains("发行")).filter(!_.split("\u0001")(1).contains("宣传")).filter(!_.split("\u0001")(1).contains("宣布")).filter(!_.split("\u0001")(2).equals("")).filter(!_.split("\u0001")(3).equals("")).map(x=>{x.split("\u0001")(0)+"\u0001"+x.split("\u0001")(1)+"\u0001"+x.split("\u0001")(2)}).sample(false,0.15,1).saveAsTextFile("wangteng/newsRelease/negativeData")
    sc.textFile("wangteng/output/ReleaseNews/*").filter(_.split("\u0001").length>3).filter(!_.split("\u0001")(2).equals("")).filter(!_.split("\u0001")(3).equals("")).filter(!_.split("\u0001")(1).contains("现身")).filter(!_.split("\u0001")(1).contains("出席")).repartition(1).saveAsTextFile("wangteng/ReleaseNews")

  }

}


