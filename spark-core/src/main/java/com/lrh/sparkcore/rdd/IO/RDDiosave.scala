package com.lrh.sparkcore.rdd.IO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDiosave {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("io")
    val sc = new SparkContext(sparkConf)

    //从文件中创建RDD
    val rdd = sc.makeRDD(List(
      ("a",1),
      ("b",2),
      ("c",3)
    ))

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }

}
