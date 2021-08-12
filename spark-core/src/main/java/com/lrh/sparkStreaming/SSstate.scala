package com.lrh.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSstate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val word: DStream[(String, Int)] = words.map((_, 1))

    val state = word.updateStateByKey(
      ( seq:Seq[Int], buff:Option[Int] ) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    state.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
