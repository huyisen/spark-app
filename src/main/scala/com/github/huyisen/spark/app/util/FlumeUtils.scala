package com.github.huyisen.spark.app.util

import java.util.concurrent.TimeUnit

import com.google.gson.Gson
import org.apache.spark.Logging

import scala.collection.JavaConverters._
import scala.collection.{Iterator, mutable}
import scala.util.Random

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-11 00:50
  * <p>Version: 1.0
  */
object FlumeUtils extends Logging {

  val groupSize = 100

  def main(args: Array[String]): Unit = {

    while (true) {
      val data = new mutable.ArrayBuffer[mutable.Map[String, String]]()
      val timestamp = System.currentTimeMillis().toString

      for (x <- 0 until 3) {
        data += mutable.Map[String, String]("ID" -> (new Random).nextInt(1000000).toString, "Author" -> x.toString, "Time" -> timestamp)
//        data += mutable.Map[String, String]("ID" -> x.toString, "Author" -> x.toString, "Time" -> timestamp)
//        data += mutable.Map[String, String]("ID" -> x.toString, "Author" -> x.toString, "Time" -> timestamp)
//        data += mutable.Map[String, String]("ID" -> x.toString, "Author" -> x.toString, "Time" -> timestamp)
//        data += mutable.Map[String, String]("ID" -> x.toString, "Author" -> x.toString, "Time" -> timestamp)
      }

      val result = post("115.231.251.252", 26016, "spark", data)
      println(" ===>  result --> " + result)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  def post(ip: String, port: Int, topic: String, data: mutable.Buffer[mutable.Map[String, String]],
           compress: Boolean = true, key: String = null): Unit = {

    val partitionData = if (data.length > groupSize) data.grouped(groupSize).toList else Iterator(data).toList
    val timestamp = System.currentTimeMillis
    val gson = new Gson()

    val listContainer = partitionData.map(partition => gson.toJson(
      partition.map(tuple => Container(Header(topic, timestamp), gson.toJson(tuple.asJava))).asJava)
    )

    listContainer.foreach(container => {
      println(container)
      val compressData = if (compress) CompressUtils.compress(container) else container.getBytes(Constants.CHARSET_NAME)
      HttpUtils.post("http://" + ip + ":" + port, compressData)
    })

  }

}

case class Header(topic: String, timestamp: Long, key: String = null) extends Serializable {
}

case class Container(headers: Header, body: String) extends Serializable {
}

