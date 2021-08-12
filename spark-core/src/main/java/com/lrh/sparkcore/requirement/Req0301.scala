package com.lrh.sparkcore.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/9 22:05
 *
 **/
object Req0301 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)
    val rdd = sc.textFile("datas\\user_visit_action.txt")

    val datasrdd: RDD[UserVisitAction] = rdd.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    datasrdd.cache()
    //分母
    val pagidRdd: Array[(Long, Int)] = datasrdd.map(
      action => {
        (action.page_id, 1)
      }
    ).reduceByKey(_ + _).collect()
    //分子


    sc.stop()
  }
  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,// 用 户 的 ID
                              session_id: String,//Session 的 ID
                              page_id: Long,// 某 个 页 面 的 ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,// 某 一 个 商 品 品 类 的 ID
                              click_product_id: Long,// 某 一 个 商 品 的 ID
                              order_category_ids: String,//一次订单中所有品类的 ID 集合
                              order_product_ids: String,//一次订单中所有商品的 ID 集合
                              pay_category_ids: String,//一次支付中所有品类的 ID 集合
                              pay_product_ids: String,//一次支付中所有商品的 ID 集合
                              city_id: Long
                            )//城市 id

}
