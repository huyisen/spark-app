package com.github.huyisen.spark.app.sink.kafka

import java.util.Properties

import com.github.huyisen.spark.app.pool.KafkaProducer
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

  /**
    * Sink a DStream or RDD to Kafka
    *
    * @param producerConfig properties for a KafkaProducer
    * @param transformFunc  a function used to transform values of T type into [[ProducerRecord]]s
    * @param callback       an optional [[Callback]] to be called after each write, default value is None.
    */
  def sinkToKafka[K, V](
    producerConfig: Properties,
    transformFunc: T => ProducerRecord[K, V],
    producerPool: Broadcast[GenericObjectPool[KafkaProducer]],
    callback: Option[Callback] = None
  )
}
