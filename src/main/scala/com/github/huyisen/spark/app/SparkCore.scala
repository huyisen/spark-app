package com.github.huyisen.spark.app

import java.util.Properties

import com.github.huyisen.spark.app.pool.{BaseKafkaWorkerFactory, KafkaWorker, PooledKafkaWorkerFactory}
import com.github.huyisen.spark.app.sink.kafka.RDDKafkaSink
import com.github.huyisen.spark.app.wrap.WrapperSingleton
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 17:18
  * <p>Version: 1.0
  */
class SparkCore(args: Array[String]) extends RunTools with Serializable {

  override def run(): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("ArticleSparkApp")
      .set("spark.yarn.executor.memoryOverhead", "4096")
      .set("spark.yarn.driver.memoryOverhead", "8192")
      .set("spark.akka.frameSize", "1000")
      .set("sspark.rdd.compress", "true")
      .set("spark.broadcast.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val time = System.currentTimeMillis()
    val source = sc.parallelize(Array((time, 1, "李四", 18), (time, 2, "张三", 19), (time, 3, "tom", 16), (time, 4, "jetty", 25), (time, 5, "msk", 28), (time, 6, "tina", 10)), 3)

    val brokers = "node3.com:6667"

    val pool = WrapperSingleton.apply({
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producerFactory = new BaseKafkaWorkerFactory(props, defaultTopic = Option("test"))
      val pooledProducerFactory = new PooledKafkaWorkerFactory(producerFactory)
      val poolConfig = {
        val c = new GenericObjectPoolConfig
        val maxNumProducers = 5
        c.setMaxTotal(maxNumProducers)
        c.setMaxIdle(maxNumProducers)
        c
      }
      new GenericObjectPool[KafkaWorker](pooledProducerFactory, poolConfig)
    })

    val topic = "test"
    val sink = new RDDKafkaSink[(Long, Int, String, Int)](source, pool)
    sink.sinkToKafka(tuple => new ProducerRecord(topic, tuple._1 + "," + tuple._2 + "," + tuple._3 + "," + tuple._4))

    sc.stop()
  }

}

