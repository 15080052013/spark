package com.lrh.sparkcore.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/9 22:05
 *
 **/
object Req0102 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)
    val rdd = sc.textFile("datas\\user_visit_action.txt")

    rdd.cache()


    val rdd1 = rdd.filter(
      x => {
        val datas = x.split("_")
        datas(6) != "-1"
      }
    )
    val rdd2 = rdd1.map(
      x => {
        val datas = x.split("_")
        (datas(6), 1)
      }

    ).reduceByKey(_ + _)



    val rdd11 = rdd.filter(
      x => {
        val datas = x.split("_")
        datas(8) != "null"
      }
    )
    val rdd12 = rdd11.flatMap(
      x => {
        val datas = x.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids
      }
    )
    val rdd13 = rdd12.map((_, 1)).reduceByKey(_ + _)




    val rdd21 = rdd.filter(
      x => {
        val datas = x.split("_")
        datas(10) != "null"
      }
    )
    val rdd22 = rdd21.flatMap(
      x => {
        val datas = x.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids
      }
    )
    val rdd23 = rdd22.map((_, 1)).reduceByKey(_ + _)

    val rdd01 = rdd2.map{
      case ( cid, cnt ) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd02 = rdd13.map{
      case ( cid, cnt ) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd03 = rdd23.map{
      case ( cid, cnt ) => {
        (cid, (0, 0, cnt))
      }
    }
    val res: RDD[(String, (Int, Int, Int))] = rdd01.union(rdd02).union(rdd03)
    val result: RDD[(String, (Int, Int, Int))] = res.reduceByKey(
      (x, y) => {
        (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }
    )
    result.sortBy(_._2,false).take(10).foreach(println)


    sc.stop()
  }

}
