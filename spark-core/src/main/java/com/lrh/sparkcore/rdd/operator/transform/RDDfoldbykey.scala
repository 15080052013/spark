package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDfoldbykey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)),2)

    rdd.foldByKey(0)(_+_).collect().foreach(println)
    sc.stop()
  }

}
