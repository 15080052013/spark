package com.lrh.sparkcore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/5 21:32
 *
 **/
object RDDcountbyket {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,1,3,4),2)

    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("a",3)))
    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)
    sc.stop()
  }
}
