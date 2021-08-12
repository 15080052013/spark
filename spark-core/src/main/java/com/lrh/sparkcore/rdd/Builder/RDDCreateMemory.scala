package com.lrh.sparkcore.rdd.Builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDCreateMemory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //从内存中创建RDD
    val seq = Seq[Int](0,2,3,4)
    // val rdd = sc.parallelize(seq)
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)


    sc.stop()
  }

}
