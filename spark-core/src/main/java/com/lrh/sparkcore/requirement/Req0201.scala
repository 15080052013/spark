package com.lrh.sparkcore.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req0201 {
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
    val top10Ids: Array[String] = result.sortBy(_._2, false).take(10).map(_._1)

    val filrdd: RDD[String] = rdd.filter(
      x => {
        val datas: Array[String] = x.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else
          false
      }
    )
    val maprdd: RDD[((String, String), Int)] = filrdd.map(
      x => {
        val datas: Array[String] = x.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    val maprdd1: RDD[(String, Iterable[(String, Int)])] = maprdd.map {
      case ((x, y), z) => {
        (x, (y, z))
      }
    }.groupByKey()
    maprdd1.mapValues(
      iter =>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    ).collect().foreach(println)
    sc.stop()
  }

}
