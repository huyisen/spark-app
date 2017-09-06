package com.github.huyisen.spark.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.github.huyisen.spark.app.pool.{BaseKafkaProducerFactory, KafkaProducer, PooledKafkaProducerFactory}
import com.github.huyisen.spark.app.sink.kafka.RDDKafkaSink
import com.github.huyisen.spark.app.source.KafkaSource
import com.github.huyisen.spark.app.util.PoolUtils
import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-08-28 22:30
  * <p>Version: 1.0
  */

trait RunTools extends Serializable {
  def run()
}

class SparkStreaming(args: Array[String]) extends RunTools with Serializable {

  override def run(): Unit = {

    // val args = Array("local", "Spark", "10")
    val Array(master, appName, record) = args

    val sparkConf = new SparkConf()
      .setAppName(s"SparkStream-[${appName}]-App")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", record)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    if (master.contains("local")) {
      sparkConf.setMaster("local[1]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val brokers = "shzsw52.urun:6667,shzsw53.urun:6667,shzsw54.urun:6667"
    //"largest" else "smallest"
    val reset = "largest"
    val groupId = "spark"

    val kafkaParams = Map(
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> reset,
      "group.id" -> groupId)
    val topics = Set[String]("spark")
    val source = new KafkaSource[String, String, StringDecoder, StringDecoder](kafkaParams)
    val dStream = source.createDStream(ssc, topics)

    val d1 = dStream.mapPartitions(partition =>
      partition.map(tuple => Try({
        JSON.parseObject(tuple._2).asScala
      })).collect { case Success(x) => x } // 我们只关心没有异常的。
    )

    //    val quorum = "shzsw58.urun,shzsw59.urun,shzsw60.urun"
    //    val tableName = "spark"
    //
    //    val conf = HBaseConfiguration.create
    //    conf.set("hbase.client.pause", "2000")
    //    conf.set("hbase.rpc.timeout", "120000")
    //    conf.set("hbase.client.retries.number", "50")
    //    conf.set("hbase.client.operation.timeout", "120000")
    //    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //    conf.set("hbase.zookeeper.quorum", quorum)
    //
    //    val job = Job.getInstance(conf)
    //    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //    val jobConf = job.getConfiguration //can't be serializable
    //    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //
    //    val hbaseSink = new DStreamHBaseSink[(String, String)](dStream)
    //    hbaseSink.sinkToHBase(jobConf, Transform.kafkaToHBase)
    //
    //
    //    val zkHost = ""
    //    val collection = ""
    //    val batch = "500"
    //
    //    val solrConf = {
    //      val config = new Properties
    //      config.setProperty("zkHost", "")
    //      config.setProperty("collection", "")
    //      config.setProperty("batch", "")
    //      config
    //    }
    //
    //    val broadFields = {
    //      val client = new CloudSolrClient(zkHost)
    //      client.connect()
    //      val fields = SolrUtils.getFields(client, collection)
    //      client.close()
    //      ssc.sparkContext.broadcast(fields)
    //    }
    //
    //    val producerPool = {
    //      val pool = PoolUtils.createSolrProducerPool(zkHost, solrConf, collection)
    //      ssc.sparkContext.broadcast(pool)
    //    }
    //
    //    val solrSink = new DStreamSolrSink[(String, String)](dStream, producerPool, broadFields)
    //    solrSink.sinkToSolr(solrConf, Transform.kafkaToSolr)
    //


    //update zk offset

    dStream.foreachRDD(rdd => source.updateZKOffsets(rdd))

    ssc.start()
    ssc.awaitTermination()
  }


}

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
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    val source = sc.parallelize(Array((1, "李四", 18), (2, "张三", 19), (3, "tom", 16), (4, "jetty", 25), (5, "msk", 28), (6, "tina", 10)), 3)

    val topic = "test"
    val props = new Properties()
    props.put("bootstrap.servers","192.168.56.13:6667")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val sink = new RDDKafkaSink[(Int, String, Int)](source)
    sink.sinkToKafka(props, tuple => new ProducerRecord(topic, tuple._1 + "," + tuple._2 + "," + tuple._3))

    sc.stop()
  }

}

object SparkApp {

  /**
    * 程序入口。
    *
    * @param args 参数列表
    */
  def main(args: Array[String]): Unit = {
    //    val mode = args.head
    val mode = "core"
    if ("Streaming".endsWith(mode)) {
      new SparkStreaming(args).run()
    } else {
      new SparkCore(args).run()
    }
  }

}
