package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 16:20
 *
 **/
object RDDTransformPar {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val rddmap = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    rddmap.collect().foreach(println)

    sc.stop()
  }

}
