package com.lrh.sparkcore.WC

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/3/28 16:24
 *
 **/
object Spark01_WC {
  def main(args: Array[String]): Unit = {

    //建立Spark链接

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务

    // 读取文件数据
    val fileRDD: RDD[String] = sc.textFile("datas/*")

    // 将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap( _.split(" ") )

    // 转换数据结构 word => (word, 1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    word2OneRDD.foreach(println)
    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)

    word2CountRDD.foreach(println)
    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    // 打印结果
    word2Count.foreach(println)


    //关闭连接
    sc.stop()


  }

}

