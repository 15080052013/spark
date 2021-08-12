package com.lrh.sparkcore.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @Author lrh
 * @Date 2021/3/29 17:08
 *
 **/
object Driver {

  def main(args: Array[String]): Unit = {

    val clint1 = new Socket("localhost",9999)
    val clint2 = new Socket("localhost",8888)

    val task = new Task()
    val out: OutputStream = clint1.getOutputStream
    val objOut = new ObjectOutputStream(out)

    val subTask = new SubTask()
    subTask.logic = task.logic


    objOut.writeObject(task)
    objOut.flush()
    objOut.close()
    clint1.close()
    println("数据发送完毕")
  }
}
