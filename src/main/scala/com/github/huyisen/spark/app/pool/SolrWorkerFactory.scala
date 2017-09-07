package com.github.huyisen.spark.app.pool

import java.util.Properties

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 23:21
  * <p>Version: 1.0
  */
private[app] abstract class SolrWorkerFactory(
  zkHost: String,
  config: Properties = new Properties,
  collection: Option[String] = None
) extends Serializable {

  def newInstance(): SolrWorker
}