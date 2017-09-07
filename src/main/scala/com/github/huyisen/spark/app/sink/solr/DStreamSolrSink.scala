package com.github.huyisen.spark.app.sink.solr

import java.util.Properties

import com.github.huyisen.spark.app.pool.SolrWorker
import com.github.huyisen.spark.app.wrap.WrapperSingleton
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:39
  * <p>Version: 1.0
  */
class DStreamSolrSink[T: ClassTag](
  @transient private val dStream: DStream[T],
  private val pool: WrapperSingleton[GenericObjectPool[SolrWorker]],
  fields: Broadcast[Set[String]]
) extends SolrSink[T] {

  override def sinkToSolr(
    solrConf: Properties,
    transformFunc: (T) => SolrInputDocument
  ): Unit = dStream.foreachRDD(rdd => {
    val rddSink = new RDDSolrSink[T](rdd, pool,fields)
    rddSink.sinkToSolr(solrConf, transformFunc)
  })
}
