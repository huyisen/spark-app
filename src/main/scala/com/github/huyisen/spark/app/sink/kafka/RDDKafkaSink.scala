package com.github.huyisen.spark.app.sink.kafka

import com.github.huyisen.spark.app.pool.KafkaWorker
import com.github.huyisen.spark.app.wrap.{WrapperSingleton, WrapperVariable}
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:01
  * <p>Version: 1.0
  */
class RDDKafkaSink[T: ClassTag](@transient private val rdd: RDD[T],
   private val pool: WrapperSingleton[GenericObjectPool[KafkaWorker]])
  extends KafkaSink[T] with Serializable {

  override def sinkToKafka(
    transformFunc: (T) => ProducerRecord[String, String],
    callback: Option[Callback]
  ): Unit = rdd.foreachPartition(partition => {

    val producer = pool.get.borrowObject()
    partition
      .map(transformFunc)
      .foreach(record => producer.send(record, callback))

    pool.get.returnObject(producer)
  })
}
