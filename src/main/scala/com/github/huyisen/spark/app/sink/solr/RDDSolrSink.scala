package com.github.huyisen.spark.app.sink.solr

import java.util.Properties

import com.github.huyisen.spark.app.pool.CloudSolrProducer
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:40
  * <p>Version: 1.0
  */
class RDDSolrSink[T: ClassTag](
  @transient private val rdd: RDD[T],
  producerPool: Broadcast[GenericObjectPool[CloudSolrProducer]],
  fields: Broadcast[Set[String]]
) extends SolrSink[T] {

  /**
    * Sink a DStream or RDD to HBase
    *
    * @param solrConf      properties for a SolrClient
    * @param transformFunc a function used to transform values of T type into [[SolrInputDocument]]s
    */
  override def sinkToSolr(
    solrConf: Properties,
    transformFunc: (T) => SolrInputDocument
  ): Unit = rdd.foreachPartition(partition => {

    //TODO 捕获一样处理
    val batch = solrConf.getProperty("batch", "1000").toInt
    val client = producerPool.value.borrowObject()
    partition
      .map(transformFunc)
      .map(doc => {
        val newDoc = new SolrInputDocument
        for (field <- fields.value if doc.containsKey(field))
          newDoc.setField(field, doc.getField(field))
        newDoc
      })
      .grouped(batch)
      .foreach(docs => client.add(docs))
    client.commit
    producerPool.value.returnObject(client)
  })

}
