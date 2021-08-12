package com.lrh.sparkcore.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/29 20:22
 *
 **/
object RDDpart {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("par")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(
      ("nba","4564564"),
      ("cba","55454"),
      ("wnba","556454"),
      ("nba","56564")
    ),3)
    val rdd1: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    rdd1.saveAsTextFile("output")
    sc.stop()
  }
  class MyPartitioner extends Partitioner{

    //分区数量
    override def numPartitions: Int = 3



    override def getPartition(key: Any): Int = {

      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
