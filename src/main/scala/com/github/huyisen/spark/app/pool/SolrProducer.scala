package com.github.huyisen.spark.app.pool

import java.util.Properties

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.response.UpdateResponse
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConversions._

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-19 23:16
  * <p>Version: 1.0
  */
private[app] case class CloudSolrProducer(
  zkHost: String,
  solrConfig: Properties = new Properties,
  defaultCollection: Option[String] = None,
  cloudSolrClient: Option[CloudSolrClient] = None
) {
  require(zkHost == null || !zkHost.isEmpty, "Must set zkHost")

  private val p = cloudSolrClient getOrElse {
    //    val effectiveConfig = {
    //      val c = new Properties
    //      c.load(this.getClass.getResourceAsStream("/producer-defaults.properties"))
    //      c.putAll(producerConfig)
    //      c.put("metadata.broker.list", brokerList)
    //      c
    //    }
    val cloudSolrClient = new CloudSolrClient(zkHost)

    cloudSolrClient.connect()
    cloudSolrClient
  }

  private[app] def add(docs: Seq[SolrInputDocument]): UpdateResponse = {
    p.add(docs)
  }

  private[app] def commit: UpdateResponse = {
    p.commit()
  }


  def shutdown(): Unit = p.close()

}

private[app] abstract class CloudSolrProducerFactory(
  zkHost: String,
  config: Properties = new Properties,
  collection: Option[String] = None
) extends Serializable {

  def newInstance(): CloudSolrProducer
}

private[app] class BaseCloudSolrProducerFactory(
  zkHost: String,
  config: Properties = new Properties,
  defaultCollection: Option[String] = None
) extends CloudSolrProducerFactory(zkHost, config, defaultCollection) {

  override def newInstance(): CloudSolrProducer = CloudSolrProducer(zkHost, config, defaultCollection)

}