package com.lrh.sparkcore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/5 21:32
 *
 **/
object RDDaggregate {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val resutrdd: Int = rdd.aggregate(0)(_ + _, _ + _)
    val resutrdd1: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(resutrdd)
    println(resutrdd1)
    sc.stop()
  }
}
