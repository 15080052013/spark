package com.lrh.sparkcore.test

/**
 * @Author lrh
 * @Date 2021/3/29 17:39
 *
 **/
class SubTask extends Serializable {
  var datas:List[Int] = _
  var logic:(Int)=>Int = _
  def compute() = {
    datas.map(logic)
  }

}
