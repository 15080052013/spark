package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDsortby {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("1",2),("1212",5),("5",2)), 2)
//    rdd.sortBy(x=>x._1).collect().foreach(println)
    rdd.sortBy(x=>x._1,false).collect().foreach(println)

    sc.stop()
  }

}
