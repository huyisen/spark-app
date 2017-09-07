package com.github.huyisen.spark.app

import java.util.Properties

import com.github.huyisen.spark.app.pool.{BaseKafkaProducerFactory, KafkaProducer, PooledKafkaProducerFactory}
import com.github.huyisen.spark.app.sink.kafka.DStreamKafkaSink
import com.github.huyisen.spark.app.source.KafkaSource
import com.github.huyisen.spark.app.wrap.WrapperVariable
import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 17:19
  * <p>Version: 1.0
  */
class SparkStreaming(args: Array[String]) extends RunTools with Serializable {

  override def run(): Unit = {

    val sparkConf = new SparkConf()
      //      .setMaster("local[3]")
      .setAppName(s"SparkStream-[Test]-App")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val brokers = "sdst103.urun:6667,sdst104.urun:6667,sdst106.urun:6667"
    //"largest" else "smallest"
    val reset = "smallest"
    val groupId = "test"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> reset,
      "group.id" -> groupId)
    val topics = Set[String]("test")
    val input = new KafkaSource[String, String, StringDecoder, StringDecoder](kafkaParams)
    val dStream = input.createDStream(ssc, topics)

    val source = dStream.mapPartitions(partition => {
      partition.map(src => {
        val arr = src._2.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      })
    })

    val pool = WrapperVariable.apply({

      val props = new Properties()
      props.put("bootstrap.servers", "sdst103.urun:6667,sdst104.urun:6667,sdst106.urun:6667")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producerFactory = new BaseKafkaProducerFactory(props, defaultTopic = Option("test"))
      val pooledProducerFactory = new PooledKafkaProducerFactory(producerFactory)
      val poolConfig = {
        val c = new GenericObjectPoolConfig
        val maxNumProducers = 10
        c.setMaxTotal(maxNumProducers)
        c.setMaxIdle(maxNumProducers)
        c
      }
      new GenericObjectPool[KafkaProducer](pooledProducerFactory, poolConfig)
    })

    val topic = "test"
    val sink = new DStreamKafkaSink[(Int, String, Int)](source)
    sink.sinkToKafka(pool, tuple => new ProducerRecord(topic, tuple._1 + "," + tuple._2 + "," + tuple._3))

    source.foreachRDD(rdd => {
      rdd.foreach(t => {
        println(System.currentTimeMillis() + " -> " + t)
      })
    })

    //update zk offset
    dStream.foreachRDD(rdd => input.updateZKOffsets(rdd))

    ssc.start()
    ssc.awaitTermination()
  }


}
