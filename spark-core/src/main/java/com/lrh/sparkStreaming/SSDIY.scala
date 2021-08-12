package com.lrh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSDIY {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val msgDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    msgDS.print()
    ssc.start()
    ssc.awaitTermination()

  }
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true
    override def onStart(): Unit = {

      new Thread(new Runnable {
        override def run(): Unit = {

          while (flag){
            val msg = "数据为"+new Random().nextInt(10).toString
            store(msg)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false

    }
  }


}
