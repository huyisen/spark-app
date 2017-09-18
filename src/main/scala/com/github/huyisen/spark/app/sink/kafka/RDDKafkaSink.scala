package com.github.huyisen.spark.app.sink.kafka

import java.util.Properties

import com.github.huyisen.spark.app.pool.{BaseKafkaWorkerFactory, KafkaWorker, PooledKafkaWorkerFactory}
import com.github.huyisen.spark.app.wrap.WrapperSingleton
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:01
  * <p>Version: 1.0
  */
class RDDKafkaSink[T: ClassTag](@transient private val rdd: RDD[T], private val config: Properties)
  extends KafkaSink[T] with Serializable {

   lazy val pools = WrapperSingleton.apply({
    val producerFactory = new BaseKafkaWorkerFactory(config)
    val pooledProducerFactory = new PooledKafkaWorkerFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 2
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaWorker](pooledProducerFactory, poolConfig)
  })

  override def sinkToKafka(
    transformFunc: (T) => ProducerRecord[String, String],
    callback: Option[Callback]
  ): Unit = rdd.foreachPartition(partition => {

    val producer = pools.get.borrowObject()
    partition
      .map(transformFunc)
      .foreach(record => producer.send(record, callback))

    println(" ---------------------------------------------> " + producer.singletonUUID)
    pools.get.returnObject(producer)
  })
}
