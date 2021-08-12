package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDsample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
//    rdd.sample(false,0.4,5).collect().foreach(println)
    rdd.sample(true,0.4).collect().foreach(println)
    sc.stop()
  }

}
