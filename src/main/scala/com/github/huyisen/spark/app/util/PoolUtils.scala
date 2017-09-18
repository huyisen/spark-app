package com.github.huyisen.spark.app.util

import java.util.Properties

import com.github.huyisen.spark.app.pool._
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-18 23:00
  * <p>Version: 1.0
  */
object PoolUtils {

  private[app] def createKafkaWorkerPool(
    config: Properties,
    topic: String
  ): GenericObjectPool[KafkaWorker] = {
    val producerFactory = new BaseKafkaWorkerFactory(config)
    val pooledProducerFactory = new PooledKafkaWorkerFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaWorker](pooledProducerFactory, poolConfig)
  }

  private[app] def createSolrWorkerPool(
    zkHost: String,
    config: Properties,
    collection: String
  ): GenericObjectPool[SolrWorker] = {
    val producerFactory = new BaseSolrWorkerFactory(zkHost, config, defaultCollection = Option(collection))
    val pooledProducerFactory = new PooledSolrWorkerFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[SolrWorker](pooledProducerFactory, poolConfig)
  }

}
