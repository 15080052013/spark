package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDgroupbykey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("b",2),("a",3),("c",4)))

    val rddgroup: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    rddgroup.collect().foreach(println)
    sc.stop()
  }

}
