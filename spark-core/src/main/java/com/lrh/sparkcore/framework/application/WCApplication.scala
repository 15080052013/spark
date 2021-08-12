package com.lrh.sparkcore.framework.application

import com.lrh.sparkcore.framework.controller.WCController
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/12 20:31
 *
 **/
object WCApplication extends App {

  val controller = new WCController()
  //建立Spark链接

  val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
  val sc = new SparkContext(sparkConf)
  //执行业务



  controller.execute()

  //关闭连接
  sc.stop()
}
