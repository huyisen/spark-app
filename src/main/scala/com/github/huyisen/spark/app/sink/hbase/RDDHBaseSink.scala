package com.github.huyisen.spark.app.sink.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:35
  * <p>Version: 1.0
  */
class RDDHBaseSink[T: ClassTag](@transient private val rdd: RDD[T])
  extends HBaseSink[T] {

  override def sinkToHBase(
    config: Configuration,
    transformFunc: (T) => (ImmutableBytesWritable, Put)
  ): Unit = rdd.mapPartitions(_.map(transformFunc)).saveAsNewAPIHadoopDataset(config)

}
