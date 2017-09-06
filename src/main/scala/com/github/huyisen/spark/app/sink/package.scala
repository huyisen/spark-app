package com.github.huyisen.spark.app

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

import com.github.huyisen.spark.app.sink.hbase._
import com.github.huyisen.spark.app.sink.kafka._
import com.github.huyisen.spark.app.sink.solr._

/**
  *
  * Implicit conversions
  *
  * 1)[[DStream]] -> [[HBaseSink]]
  * 2)[[RDD]] -> [[HBaseSink]]
  *
  * 3)[[DStream]] -> [[KafkaSink]]
  * 4)[[RDD]] -> [[KafkaSink]]
  *
  * 5)[[DStream]] -> [[SolrSink]]
  * 6)[[RDD]] -> [[SolrSink]]
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-11 22:54
  * <p>Version: 1.0
  */
package object sink {

  /**
    * Convert a [[DStream]] to a [[KafkaSink]] implicitly
    *
    * @param dStream [[DStream]] to be converted
    * @return [[KafkaSink]] ready to write messages to Kafka
    */
  implicit def dStreamToKafkaSink[T: ClassTag, K, V](dStream: DStream[T]): KafkaSink[T] =
    new DStreamKafkaSink[T](dStream)

  /**
    * Convert a [[RDD]] to a [[KafkaSink]] implicitly
    *
    * @param rdd [[RDD]] to be converted
    * @return [[KafkaSink]] ready to write messages to Kafka
    */
  implicit def rddToKafkaSink[T: ClassTag, K, V](rdd: RDD[T]): KafkaSink[T] =
    new RDDKafkaSink[T](rdd)

  /**
    * Convert a [[DStream]] to a [[HBaseSink]] implicitly
    *
    * @param dStream [[DStream]] to be converted
    * @return [[HBaseSink]] ready to write messages to HBase
    */
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
