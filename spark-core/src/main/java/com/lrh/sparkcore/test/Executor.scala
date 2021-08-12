package com.lrh.sparkcore.test

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @Author lrh
 * @Date 2021/3/29 17:10
 *
 **/
object Executor {
  def main(args: Array[String]): Unit = {

    val server = new ServerSocket(9999)

    println("服务器启动，等待接收数据")

    val client = server.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task = objIn.readObject().asInstanceOf[Task]
    val list = task.compute()

    println("接收到"+list)
    objIn.close()
    client.close()
    server.close()


   }
}
