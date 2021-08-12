package com.lrh.sparkcore.test

/**
 * @Author lrh
 * @Date 2021/3/29 17:21
 *
 **/
class Task extends Serializable {

  val datas = List(1,2,3,4)
//  val logic = (i:Int)=>{i*4}
  val logic = (i:Int)=>{i*4}
  def compute() = {
    datas.map(logic)
  }
}
