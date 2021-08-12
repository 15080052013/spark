package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 16:20
 *
 **/
object RDDTransform22 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(List(1, 2),3 ,List(4,5)))
    val rdd1 = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    rdd1.collect().foreach(println)

    sc.stop()
  }

}
