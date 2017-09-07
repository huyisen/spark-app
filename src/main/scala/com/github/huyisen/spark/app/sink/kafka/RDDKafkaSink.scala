package com.github.huyisen.spark.app.sink.kafka

import com.github.huyisen.spark.app.pool.KafkaProducer
import com.github.huyisen.spark.app.wrap.WrapperVariable
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
class RDDKafkaSink[T: ClassTag](@transient private val rdd: RDD[T])
  extends KafkaSink[T] with Serializable {
  /**
    * Sink a DStream or RDD to Kafka
    *
    * @param pool          properties for a KafkaProducer
    * @param transformFunc a function used to transform values of T type into [[ProducerRecord]]s
    * @param callback      an optional [[Callback]] to be called after each write, default value is None.
    */
  override def sinkToKafka(
                            pool: WrapperVariable[GenericObjectPool[KafkaProducer]],
                            transformFunc: (T) => ProducerRecord[String, String],
                            callback: Option[Callback]
                          ): Unit = rdd.foreachPartition(partition => {

    val producer = pool.get.borrowObject()
    println("producer ==>" + producer)
    partition
      .map(transformFunc)
      .foreach(record => producer.send(record, callback))
    pool.get.returnObject(producer)
  })
}
