package com.lrh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSstate_window {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.map((_, 1))
//    val windowDS = words.window(Seconds(6))
    val windowDS = words.window(Seconds(6),Seconds(6))
    val WC: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)
    WC.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
