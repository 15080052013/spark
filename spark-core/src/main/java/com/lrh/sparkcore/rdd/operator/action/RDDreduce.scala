package com.lrh.sparkcore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/5 21:32
 *
 **/
object RDDreduce {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4))

    println(rdd.reduce(_ + _))

    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))


    println(rdd.count())

    println(rdd.first())

    println(rdd.take(3).mkString(","))


    val rdd1 = sc.makeRDD(List(5,4,8,1))
    println(rdd1.takeOrdered(3).mkString(","))
    sc.stop()
  }
}
