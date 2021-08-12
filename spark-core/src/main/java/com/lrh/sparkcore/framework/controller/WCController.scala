package com.lrh.sparkcore.framework.controller

import com.lrh.sparkcore.framework.application.WCApplication.sc
import com.lrh.sparkcore.framework.service.WCservice
import org.apache.spark.rdd.RDD

/**
 * @Author lrh
 * @Date 2021/4/12 20:31
 *
 **/
class WCController {

  private val cservice = new WCservice()
  def execute() :Unit = {
    // 读取文件数据


    val analysis = cservice.dataAnalysis
    // 打印结果
    analysis.foreach(println)
  }
}
