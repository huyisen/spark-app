package com.github.huyisen.spark.app

import java.util.Properties

import com.github.huyisen.spark.app.pool.{BaseKafkaProducerFactory, KafkaProducer, PooledKafkaProducerFactory}
import com.github.huyisen.spark.app.sink.kafka.RDDKafkaSink
import com.github.huyisen.spark.app.wrap.WrapperVariable
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

    val source = sc.parallelize(Array((1, "李四", 18), (2, "张三", 19), (3, "tom", 16), (4, "jetty", 25), (5, "msk", 28), (6, "tina", 10)), 3)


    val pool = WrapperVariable.apply({

      val props = new Properties()
      props.put("bootstrap.servers", "sdst104.urun:6667,sdst105.urun:6667,sdst106.urun:6667")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producerFactory = new BaseKafkaProducerFactory(props, defaultTopic = Option("test"))
      val pooledProducerFactory = new PooledKafkaProducerFactory(producerFactory)
      val poolConfig = {
        val c = new GenericObjectPoolConfig
        val maxNumProducers = 2
        c.setMaxTotal(maxNumProducers)
        c.setMaxIdle(maxNumProducers)
        c
      }
      new GenericObjectPool[KafkaProducer](pooledProducerFactory, poolConfig)
    })

    val topic = "test"
    val sink = new RDDKafkaSink[(Int, String, Int)](source)
    sink.sinkToKafka(pool, tuple => new ProducerRecord(topic, tuple._1 + "," + tuple._2 + "," + tuple._3))

    sc.stop()
  }

}

