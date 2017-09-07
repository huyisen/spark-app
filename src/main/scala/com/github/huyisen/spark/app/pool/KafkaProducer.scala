package com.github.huyisen.spark.app.pool

import java.util.Properties

import org.apache.kafka.clients.producer.{ Callback, ProducerRecord }

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-19 23:16
  * <p>Version: 1.0
  */
private[app] case class KafkaProducer(
  producerConfig: Properties = new Properties,
  defaultTopic: Option[String] = None,
  producer: Option[org.apache.kafka.clients.producer.KafkaProducer[String, String]] = None
) {

  private val p = producer getOrElse {
    val effectiveConfig = {
      val c = new Properties
//      c.load(this.getClass.getResourceAsStream("/producer-defaults.properties"))
      c.putAll(producerConfig)
//      c.put("metadata.broker.list", brokerList)
      c
    }
    new org.apache.kafka.clients.producer.KafkaProducer[String, String](effectiveConfig)
  }

  def send(record: ProducerRecord[String, String], callback: Option[Callback] = None) {
    p.send(record, callback.orNull)
  }

  def shutdown(): Unit = p.close()

}


private[app] abstract class KafkaProducerFactory(
  config: Properties,
  topic: Option[String] = None
) extends Serializable {

  def newInstance(): KafkaProducer
}

private[app] class BaseKafkaProducerFactory(
  config: Properties,
  defaultTopic: Option[String] = None
) extends KafkaProducerFactory(config, defaultTopic) {

  override def newInstance() = new KafkaProducer(config, defaultTopic)

}