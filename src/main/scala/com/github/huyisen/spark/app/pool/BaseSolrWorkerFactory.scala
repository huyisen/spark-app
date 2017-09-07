package com.github.huyisen.spark.app.pool

import java.util.Properties

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 23:22
  * <p>Version: 1.0
  */
private[app] class BaseSolrWorkerFactory(
  zkHost: String,
  config: Properties = new Properties,
  defaultCollection: Option[String] = None
) extends SolrWorkerFactory(zkHost, config, defaultCollection) {

  override def newInstance(): SolrWorker = SolrWorker(zkHost, config, defaultCollection)

}
