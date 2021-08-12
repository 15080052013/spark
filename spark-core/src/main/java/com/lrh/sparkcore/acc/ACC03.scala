package com.lrh.sparkcore.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/9 16:30
 *
 **/
object ACC03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("par")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
//    val rd1 = rdd.reduce(_ + _)
//    println(rd1)
    val sum = sc.longAccumulator("sum")
    rdd.map(
      num => {
        sum.add(num)
        num
      }
    ).collect()
    println(sum.value)


    sc.stop()
  }

}
