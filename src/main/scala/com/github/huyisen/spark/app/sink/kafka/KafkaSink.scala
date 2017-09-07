package com.github.huyisen.spark.app.sink.kafka

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-11 23:02
  * <p>Version: 1.0
  */
abstract class KafkaSink[T: ClassTag] extends Serializable {

  def sinkToKafka(
    transformFunc: T => ProducerRecord[String, String],
    callback: Option[Callback] = None
  )
}
