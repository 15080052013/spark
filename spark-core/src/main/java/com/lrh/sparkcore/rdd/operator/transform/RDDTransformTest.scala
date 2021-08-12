package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 16:20
 *
 **/
object RDDTransformTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.textFile("datas/apache.log")
    val rddMap = rdd.map(x => {
      var datas = x.split(" ")
      datas(6)
    })
    rddMap.collect().foreach(println)
    sc.stop()
  }

}
