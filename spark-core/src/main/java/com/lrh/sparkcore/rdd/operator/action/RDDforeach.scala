package com.lrh.sparkcore.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author lrh
 * @Date 2021/4/5 21:32
 *
 **/
object RDDforeach {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4),2)


    val user = new User()
    rdd.foreach(
      num =>{
        println("age = "+(user.age + num))
      }
    )

    sc.stop()
  }
//  class User extends Serializable {
  case class User(){
    var age :Int = 30

  }
}
