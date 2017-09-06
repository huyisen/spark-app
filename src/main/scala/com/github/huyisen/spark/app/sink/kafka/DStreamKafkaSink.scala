package com.github.huyisen.spark.app.sink.kafka

import java.util.Properties

import com.github.huyisen.spark.app.pool.KafkaProducer
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:14
  * <p>Version: 1.0
  */
class DStreamKafkaSink[T: ClassTag](@transient private val dStream: DStream[T])
  extends KafkaSink[T] {
  /**
    * Write a DStream or RDD to Kafka
    *
    * @param producerConfig properties for a KafkaProducer
    * @param transformFunc  a function used to transform values of T type into [[ProducerRecord]]s
    * @param callback       an optional [[Callback]] to be called after each write, default value is None.
    */
  override def sinkToKafka[K, V](
    producerConfig: Properties,
    transformFunc: T => ProducerRecord[K, V],
    producerPool: Broadcast[GenericObjectPool[KafkaProducer]],
    callback: Option[Callback]
  ): Unit = dStream.foreachRDD(rdd => {
    val rddSink = new RDDKafkaSink[T](rdd)
    rddSink.sinkToKafka(producerConfig, transformFunc,producerPool, callback)
  })
}
