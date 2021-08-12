package com.lrh.sparkcore.rdd.Builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDCreateMemoryPar {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //设置默认分区
    sparkConf.set("spark.default.parallelism","5")
    val sc = new SparkContext(sparkConf)

    //RDD的并行度&分区
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    rdd.saveAsTextFile("output")

    sc.stop()
  }

}
