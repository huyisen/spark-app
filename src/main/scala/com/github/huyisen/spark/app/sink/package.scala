package com.github.huyisen.spark.app

import java.util.Properties

import com.github.huyisen.spark.app.pool.KafkaWorker
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
import com.github.huyisen.spark.app.sink.hbase._
import com.github.huyisen.spark.app.sink.kafka._
import com.github.huyisen.spark.app.sink.solr._
import com.github.huyisen.spark.app.wrap.{WrapperSingleton, WrapperVariable}
import org.apache.commons.pool2.impl.GenericObjectPool

/**
  *
  * Implicit conversions
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-11 22:54
  * <p>Version: 1.0
  */
package object sink {

  implicit def dStreamToKafkaSink[T: ClassTag, K, V](dStream: DStream[T], config: Properties): KafkaSink[T] =
    new DStreamKafkaSink[T](dStream,config)


  implicit def rddToKafkaSink[T: ClassTag, K, V](rdd: RDD[T],config: Properties): KafkaSink[T] =
    new RDDKafkaSink[T](rdd,config)


  implicit def dStreamToHBaseSink[T: ClassTag, K, V](dStream: DStream[T]): HBaseSink[T] =
    new DStreamHBaseSink[T](dStream)

  /**
    * Convert a [[RDD]] to a [[HBaseSink]] implicitly
    *
    * @param rdd [[RDD]] to be converted
    * @return [[HBaseSink]] ready to write messages to HBase
    */
  implicit def rddToHBaseSink[T: ClassTag, K, V](rdd: RDD[T]): HBaseSink[T] =
    new RDDHBaseSink[T](rdd)

}
