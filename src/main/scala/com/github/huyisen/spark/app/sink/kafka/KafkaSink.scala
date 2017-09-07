package com.github.huyisen.spark.app.sink.kafka

import java.util.Properties

import com.github.huyisen.spark.app.pool.KafkaProducer
import com.github.huyisen.spark.app.wrap.WrapperVariable
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-11 23:02
  * <p>Version: 1.0
  */
abstract class KafkaSink[T: ClassTag] extends Serializable {

  def sinkToKafka(
                   pool: WrapperVariable[GenericObjectPool[KafkaProducer]],
                   transformFunc: T => ProducerRecord[String, String],
                   callback: Option[Callback] = None
  )
}
