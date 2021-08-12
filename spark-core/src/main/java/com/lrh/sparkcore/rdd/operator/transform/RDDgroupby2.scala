package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDgroupby2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/apache.log")

    val rddg = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        val str = time.substring(11, 13)
        (str, 1)
      }
    ).reduceByKey(_ + _)
    rddg.collect().foreach(println)
    sc.stop()
  }

}
