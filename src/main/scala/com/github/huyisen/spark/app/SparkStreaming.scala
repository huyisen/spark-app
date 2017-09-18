package com.github.huyisen.spark.app

import java.net.URLDecoder
import java.util.Properties

import com.github.huyisen.spark.app.pool.{BaseKafkaWorkerFactory, KafkaWorker, PooledKafkaWorkerFactory}
import com.github.huyisen.spark.app.sink.kafka.DStreamKafkaSink
import com.github.huyisen.spark.app.source.KafkaSource
import com.github.huyisen.spark.app.wrap.WrapperSingleton
import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.parsing.json.{JSON, JSONObject}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 17:19
  * <p>Version: 1.0
  */
class SparkStreaming(args: Array[String]) extends RunTools with Serializable {

  override def run(): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName(s"SparkStream-[Test]-App")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val brokers = "qdsw31.urun:6667,qdsw32.urun:6667,qdsw34.urun:6667"
    //"largest" else "smallest"
    val reset = "smallest"
    val groupId = "test"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> reset,
      "group.id" -> groupId)
    val topics = Set[String]("nginxLogQdsw")
    val input = new KafkaSource[String, String, StringDecoder, StringDecoder](kafkaParams)
    val dStream = input.createDStream(ssc, topics)

    val source = dStream.mapPartitions(partition => {
      partition.map(src => {
        val map = new mutable.HashMap[String, Any]
        JSON.parseFull(src._2).get.asInstanceOf[Map[String, Any]].foreach(kv => {
          if ("pos".equalsIgnoreCase(kv._1)) {
            val pos = URLDecoder.decode(String.valueOf(kv._2), "UTF-8")
            map ++ pos.split("&").map(m => {
              val kv = m.split("=")
              if (kv.length == 2) {
                var v = kv(1).replace("[", "|")
                  .replace("]", "|")
                  .replace("(", "|")
                  .replace(")", "|")
                  .replace("（", "|")
                  .replace("）", "|")
                  .replace("+", "|")
                  .replaceAll("\\|+", "|")
                if (v.startsWith("|")) v = v.substring(v.indexOf("|") + 1)
                if (v.endsWith("|")) v = v.substring(0, v.length - 1)
                map += (kv(0).toLowerCase -> v.trim)
              }
            })
          } else if ("ges".equalsIgnoreCase(kv._1)){
            val ges = URLDecoder.decode(String.valueOf(kv._2), "UTF-8")
            map ++ ges.split("&").map(m => {
              val kv = m.split("=")
              if (kv.length == 2) {
                var v = kv(1).replace("[", "|")
                  .replace("]", "|")
                  .replace("(", "|")
                  .replace(")", "|")
                  .replace("（", "|")
                  .replace("）", "|")
                  .replace("+", "|")
                  .replaceAll("\\|+", "|")
                if (v.startsWith("|")) v = v.substring(v.indexOf("|") + 1)
                if (v.endsWith("|")) v = v.substring(0, v.length - 1)
                map += (kv(0).toLowerCase -> v.trim)
              }
            })
          } else {
            val t = if (kv._2.isInstanceOf[String]) {
              kv._1 -> String.valueOf(kv._2)
            } else kv._1 -> kv._2
            map += t
          }
        })
        JSONObject(map.toMap).toString()
      })
    })

    val sinkTopic = "nginxLogSparkQdsw"

    lazy val pool = WrapperSingleton.apply({
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producerFactory = new BaseKafkaWorkerFactory(props)
      val pooledProducerFactory = new PooledKafkaWorkerFactory(producerFactory)
      val poolConfig = {
        val c = new GenericObjectPoolConfig
        val maxNumProducers = 2
        c.setMaxTotal(maxNumProducers)
        c.setMaxIdle(maxNumProducers)
        c
      }
      new GenericObjectPool[KafkaWorker](pooledProducerFactory, poolConfig)
    })

    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val sink = new DStreamKafkaSink[String](source,props)
    sink.sinkToKafka(tuple => new ProducerRecord(sinkTopic, tuple))

    source.foreachRDD(rdd => {
      rdd.foreach(str => {
        println(" -> " + str)
      })
    })
    //update zk offset
    dStream.foreachRDD(rdd => input.updateZKOffsets(rdd))
    ssc.start()
    ssc.awaitTermination()
  }


}
