package com.lrh.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SaveMode, SparkSession, functions}

/**
 * @Author lrh
 * @Date 2021/4/19 16:36
 *
 **/
object JDBC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark")
      .option("driver", "com.mysql.jdbc.Driver").option("user", "root")
      .option("password", "admin")
      .option("dbtable", "user")
      .load()
    df.show()

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark")
      .option("driver", "com.mysql.jdbc.Driver").option("user", "root")
      .option("password", "admin")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()
    spark.close()
  }




}
