package com.github.huyisen.spark.app.pool

import java.util.Properties

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 23:17
  * <p>Version: 1.0
  */
private[app] class BaseKafkaWorkerFactory(config: Properties)
  extends KafkaWorkerFactory(config) {

  override def newInstance() = new KafkaWorker(config)

}
