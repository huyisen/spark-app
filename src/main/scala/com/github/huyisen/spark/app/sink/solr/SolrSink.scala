package com.github.huyisen.spark.app.sink.solr

import java.util.Properties

import org.apache.solr.common.SolrInputDocument

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:39
  * <p>Version: 1.0
  */
abstract class SolrSink[T: ClassTag] extends Serializable {
  
  def sinkToSolr(
    config: Properties,
    transformFunc: T => SolrInputDocument
  )
}
