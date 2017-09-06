package com.github.huyisen.spark.app.pool

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{ BasePooledObjectFactory, PooledObject }

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-19 23:17
  * <p>Version: 1.0
  */
private [app] class PooledKafkaProducerFactory(val factory: KafkaProducerFactory)
  extends BasePooledObjectFactory[KafkaProducer] with Serializable {

  override def create(): KafkaProducer = factory.newInstance()

  override def wrap(obj: KafkaProducer): PooledObject[KafkaProducer] = new DefaultPooledObject(obj)

  override def destroyObject(p: PooledObject[KafkaProducer]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}