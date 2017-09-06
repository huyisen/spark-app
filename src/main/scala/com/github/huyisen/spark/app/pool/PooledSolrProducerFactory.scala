package com.github.huyisen.spark.app.pool

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{ BasePooledObjectFactory, PooledObject }

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-19 23:17
  * <p>Version: 1.0
  */
private[app] class PooledSolrProducerFactory(val factory: CloudSolrProducerFactory)
  extends BasePooledObjectFactory[CloudSolrProducer] with Serializable {
  override def create(): CloudSolrProducer = factory.newInstance()

  override def wrap(obj: CloudSolrProducer): PooledObject[CloudSolrProducer] = new DefaultPooledObject(obj)

  override def destroyObject(p: PooledObject[CloudSolrProducer]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}
