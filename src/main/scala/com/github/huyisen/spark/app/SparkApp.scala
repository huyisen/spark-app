package com.github.huyisen.spark.app

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-08-28 22:30
  * <p>Version: 1.0
  */
object SparkApp {

  /**
    * 程序入口。
    *
    * @param args 参数列表
    */
  def main(args: Array[String]): Unit = {
    //    val mode = args.head
    val mode = "Streaming"
//        val mode = "core"
    if ("Streaming".endsWith(mode)) {
      new SparkStreaming(args).run()
    } else {
      new SparkCore(args).run()
    }
  }

}
