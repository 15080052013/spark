package com.lrh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSstate_transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    ssc.socketTextStream("localhost",9999)

    ssc.start()

    ssc.awaitTermination()

  }

}
