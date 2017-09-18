package com.github.huyisen.spark.app.sink.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:14
  * <p>Version: 1.0
  */
class DStreamKafkaSink[T: ClassTag](@transient private val dStream: DStream[T], private val config: Properties)
  extends KafkaSink[T] {

  override def sinkToKafka(
    transformFunc: T => ProducerRecord[String, String],
    callback: Option[Callback]
  ): Unit = dStream.foreachRDD(rdd => {
    val rddSink = new RDDKafkaSink[T](rdd, config)
    rddSink.sinkToKafka(transformFunc, callback)
  })
}
