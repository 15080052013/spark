package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDjoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a",1),("a",2),("b",3)))
    val rdd2 = sc.makeRDD(List(("a",3),("a",1),("b",2)))
    rdd1.join(rdd2).collect().foreach(println)
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)

    sc.stop()
  }

}
