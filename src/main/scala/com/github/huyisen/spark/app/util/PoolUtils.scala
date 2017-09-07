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

  private[app] def createKafkaProducerPool(
    config: Properties,
    topic: String
  ): GenericObjectPool[KafkaProducer] = {
    val producerFactory = new BaseKafkaProducerFactory(config, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaProducer](pooledProducerFactory, poolConfig)
  }

  private[app] def createSolrProducerPool(
    zkHost: String,
    config: Properties,
    collection: String
  ): GenericObjectPool[CloudSolrProducer] = {
    val producerFactory = new BaseCloudSolrProducerFactory(zkHost, config, defaultCollection = Option(collection))
    val pooledProducerFactory = new PooledSolrProducerFactory(producerFactory)
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = 10
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[CloudSolrProducer](pooledProducerFactory, poolConfig)
  }

}
