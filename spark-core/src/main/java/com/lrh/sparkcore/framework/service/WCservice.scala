package com.lrh.sparkcore.framework.service

import com.lrh.sparkcore.framework.application.WCApplication.sc
import com.lrh.sparkcore.framework.dao.WCDao
import org.apache.spark.rdd.RDD

/**
 * @Author lrh
 * @Date 2021/4/12 20:32
 *
 **/
class WCservice {

  private val dao = new WCDao()
  def dataAnalysis ={
    val lines = dao.readFile("datas/1.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word=>(word,1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    val array: Array[(String, Int)] = wordToSum.collect()
    array
  }
}
