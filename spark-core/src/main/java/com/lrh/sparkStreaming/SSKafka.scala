package com.lrh.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val kafkaPara: Map[String, Object] = Map[String, Object]( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
      "hadoop102:9092,hadoop103:9092,hadoop104:9092", ConsumerConfig.GROUP_ID_CONFIG -> "lrh",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("lrh"), kafkaPara)
    )
    kafkaDataDS.map(_.value()).print()
    ssc.start()

    ssc.awaitTermination()

  }

}
