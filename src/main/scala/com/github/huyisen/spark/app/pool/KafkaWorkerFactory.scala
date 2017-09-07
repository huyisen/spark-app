package com.github.huyisen.spark.app.pool

import java.util.Properties

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 23:16
  * <p>Version: 1.0
  */
private[app] abstract class KafkaWorkerFactory(
  config: Properties,
  topic: Option[String] = None
) extends Serializable {

  def newInstance(): KafkaWorker
}