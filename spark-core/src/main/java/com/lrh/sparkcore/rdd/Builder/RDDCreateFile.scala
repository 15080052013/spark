package com.lrh.sparkcore.rdd.Builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDCreateFile {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD
    val rdd: RDD[String] = sc.textFile("datas")
    rdd.collect().foreach(println)

    val rdd1: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd1.collect().foreach(println)
    sc.stop()
  }

}
