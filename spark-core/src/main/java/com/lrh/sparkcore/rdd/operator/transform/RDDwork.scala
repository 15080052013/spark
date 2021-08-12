package com.lrh.sparkcore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/4 19:02
 *
 **/
object RDDwork {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("datas/agent.log")
    val maprdd: RDD[((String, String), Int)] = rdd.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    val reducerdd: RDD[((String, String), Int)] = maprdd.reduceByKey(_ + _)

    val newmaprdd: RDD[(String, (String, Int))] = reducerdd.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }

    val grouprdd = newmaprdd.groupByKey()
    grouprdd.foreach(println)
    val resutrdd: RDD[(String, List[(String, Int)])] = grouprdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resutrdd.collect().foreach(println)


    sc.stop()
  }

}
