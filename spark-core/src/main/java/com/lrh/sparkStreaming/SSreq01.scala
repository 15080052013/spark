package com.lrh.sparkStreaming

import java.sql.ResultSet
import java.text.SimpleDateFormat

import com.lrh.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author lrh
 * @Date 2021/4/26 15:58
 *
 **/
object SSreq01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "lrh",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("lrh"), kafkaPara)
    )
    val adClick: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val ds = adClick.transform(
      rdd => {
        // TODO 通过JDBC周期性获取黑名单数据
        val blackList = ListBuffer[String]()

        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")

        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()
        // TODO 判断点击用户是否在黑名单中
        val filterRDD = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )

        // TODO 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad

            ((day, user, ad), 1) // (word, count)
          }
        ).reduceByKey(_ + _)
      }
    )







    ssc.start()
    ssc.awaitTermination()

  }

  case class AdClickData(ts:String,area:String,city:String,user:String,ad:String)
}
