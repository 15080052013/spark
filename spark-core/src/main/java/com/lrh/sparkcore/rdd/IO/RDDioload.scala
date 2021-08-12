package com.lrh.sparkcore.rdd.IO

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDioload {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("io")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD

    val rdd1 = sc.textFile("output1")

    println(rdd1.collect().mkString(","))
    val rdd2 = sc.objectFile[(String,Int)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3 = sc.sequenceFile[String,Int]("output3")
    println(rdd3.collect().mkString(","))




    sc.stop()
  }

}
