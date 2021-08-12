package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 16:20
 *
 **/
object RDDTransform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val rddMap = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 0) {
          iter
        }
        else {
          Nil.iterator
        }
      }
    )
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rddmap1 = rdd1.mapPartitionsWithIndex(
      (index, iter1) => {
        iter1.map(
          num => {
            (index, num)
          }
        )
      })

//    rddMap.collect().foreach(println)
    rddmap1.collect().foreach(println)
    sc.stop()
  }

}
