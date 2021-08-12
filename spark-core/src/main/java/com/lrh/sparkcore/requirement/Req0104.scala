package com.lrh.sparkcore.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Author lrh
 * @Date 2021/4/9 22:05
 *
 **/
object Req0104 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)
    val rdd = sc.textFile("datas\\user_visit_action.txt")

    val flatrdd: RDD[(String, (Int, Int, Int))] = rdd.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val sts = datas(8).split(",")
          sts.map(x => (x, (0, 1, 0)))
        } else if (datas(10) != "null") {
          val sts = datas(10).split(",")
          sts.map(x => (x, (0, 0, 1)))
        }
        else {
          Nil
        }
      }
    )
    val result: RDD[(String, (Int, Int, Int))] = flatrdd.reduceByKey(
      (x, y) => {
        (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }
    )

    result.sortBy(_._2,false).take(10).foreach(println)


    sc.stop()
  }

  case class HotCategory(cid:String, var clickCnt : Int, var orderCnt : Int, var payCnt : Int)

  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{
    private val hcMap = mutable.Map[String,HotCategory]()
    override def isZero: Boolean = {

      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()

    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {

      hcMap
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {


    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}
