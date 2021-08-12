package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDpartitionby {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val maprdd = rdd.map((_,1))
    maprdd.partitionBy(new HashPartitioner(6)).saveAsTextFile("output")

    sc.stop()
  }

}
