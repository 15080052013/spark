package com.lrh.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author lrh
 * @Date 2021/4/19 16:36
 *
 **/
object base {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")

//    df.show

    //sql
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show
    spark.sql("select avg(age) from user").show
    spark.sql("select age from user").show

    //DSL
    df.select("age","username").show
    df.select('age + 1).show

    val list = List(1,2,3,4)
    val ds: Dataset[Int] = list.toDS()
    ds.show()


    //df<=> rdd
    val rdd = spark.sparkContext.makeRDD(List((1,"zs",30),(2,"ls",40)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    val rdd1: RDD[Row] = df1.rdd
    //df<=>ds
    val ds1: Dataset[User] = df1.as[User]
    ds1.toDF()
    //ds<=>rdd
    val dsds: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd2: RDD[User] = dsds.rdd


    spark.close()
  }
  case class User(id:Int,name:String,age:Int)

}
