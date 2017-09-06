package com.github.huyisen.spark.app.sink.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:35
  * <p>Version: 1.0
  */
abstract class HBaseSink[T: ClassTag] extends Serializable {

  /**
    * Sink a DStream or RDD to HBase
    *
    * @param config        properties for a HBase
    * @param transformFunc a function used to transform values of T type into [[(ImmutableBytesWritable, Put)]]s
    */
  def sinkToHBase(
    config: Configuration,
    transformFunc: T => (ImmutableBytesWritable, Put)
  )
}
