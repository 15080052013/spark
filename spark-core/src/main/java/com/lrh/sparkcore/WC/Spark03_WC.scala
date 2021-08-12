package com.lrh.sparkcore.WC

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/28 16:24
 *
 **/
object Spark03_WC {
  def main(args: Array[String]): Unit = {

    //建立Spark链接

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    wordcount(sc)
    //关闭连接
    sc.stop()


  }
  def wordcount(sc : SparkContext):Unit ={
//    val rdd = sc.makeRDD(List)
  }

}

