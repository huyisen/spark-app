package com.github.huyisen.spark.app.pool

import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-19 23:17
  * <p>Version: 1.0
  */
private[app] class PooledSolrWorkerFactory(val factory: SolrWorkerFactory)
  extends BasePooledObjectFactory[SolrWorker] with Serializable {

  override def create(): SolrWorker = factory.newInstance()

  override def wrap(obj: SolrWorker): PooledObject[SolrWorker] = new DefaultPooledObject(obj)

  override def destroyObject(p: PooledObject[SolrWorker]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }

}
