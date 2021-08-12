package com.lrh.sparkcore.rdd.Builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDCreateFilePar {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //默认最小2
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }

}
