package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDjihe {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val rdd1 = sc.makeRDD(List(3,4,5,6))
    //交集
    val rdd2 = rdd.intersection(rdd1)
    println(rdd2.collect().mkString(","))
    //并集
    val rdd3 = rdd.union(rdd1)
    println(rdd3.collect().mkString(","))
    //差集
    val rdd4 = rdd.subtract(rdd1)
    println(rdd4.collect().mkString(","))

    //拉链
    val rdd5 = rdd.zip(rdd1)
    println(rdd5.collect().mkString(","))


    sc.stop()
  }

}
