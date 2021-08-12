package com.lrh.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author lrh
 * @Date 2021/4/19 16:36
 *
 **/
object UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val ds = spark.read.json("datas/user.json")
    ds.createOrReplaceTempView("user")
    spark.udf.register("pf",(name:String)=>{
      "name:"+name
    })
    spark.sql("select age ,pf(username) from user").show

    spark.close()
  }

}
