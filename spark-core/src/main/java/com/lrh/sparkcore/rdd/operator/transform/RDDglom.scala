package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDglom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val rddglom: RDD[Array[Int]] = rdd.glom()
    rddglom.collect().foreach(data => println(data.mkString("p")))
    sc.stop()
  }

}
