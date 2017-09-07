package com.github.huyisen.spark.app.sink.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-12 00:35
  * <p>Version: 1.0
  */
class DStreamHBaseSink[T: ClassTag](@transient private val dStream: DStream[T])
  extends HBaseSink[T] {

  override def sinkToHBase(
    config: Configuration,
    transformFunc: (T) => (ImmutableBytesWritable, Put)
  ): Unit = dStream.foreachRDD(rdd => {
    val rddSink = new RDDHBaseSink[T](rdd)
    rddSink.sinkToHBase(config, transformFunc)
  })

}
