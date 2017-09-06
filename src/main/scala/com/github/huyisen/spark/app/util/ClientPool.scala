package com.github.huyisen.spark.app.util

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpClientUtil, HttpSolrClient}
import org.apache.solr.common.cloud.Slice
import org.apache.solr.common.params.ModifiableSolrParams

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-16 22:52
  * <p>Version: 1.0
  */
class ClientPool

object ClientPool extends Serializable {

  def apply(unit: Unit) = new ClientPool

  val clientPool = new ConcurrentHashMap[String, SolrClient]()
  val slices = new mutable.HashSet[Slice]
  var dateSlices: DateSlices = _

  def setClients(map: Map[String, SolrClient]): Unit = {
    map.foreach(tuple => {
      clientPool.put(tuple._1, tuple._2)
    })
  }

  def getClient(shard: String): SolrClient = {
    clientPool(shard)
  }

  def setSlices(slices: Iterable[Slice]): Unit = {
    slices.foreach(slice => this.slices.add(slice))
  }

  def getSlices(zkHost: String, collection: String): mutable.HashSet[Slice] = {

    if (slices.isEmpty) {
      synchronized(this) {
        val clientPool = slices.map(slice => {
          val client = new CloudSolrClient(zkHost)
          client.setDefaultCollection(collection)
          client.connect()
          (slice.getName, client)
        }).toMap
        ClientPool.setClients(clientPool)
        ClientPool.setSlices(slices)
      }
    }

    slices
  }

  def setDateSlices(dateSlices: DateSlices): Unit = {
    this.dateSlices = dateSlices
  }

  def getDateSlices(zkHost: String, collection: String): DateSlices = {

    if (dateSlices == null) {
      synchronized(this) {
        val client = new CloudSolrClient(zkHost)
        client.setDefaultCollection(collection)
        client.connect()
        val zkReader = client.getZkStateReader
        val docCollection = zkReader.getClusterState.getCollection(collection)
        val slices = docCollection.getSlices
        client.close()
        val pool = slices.map(slice => {
          val params = new ModifiableSolrParams
          params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 500)
          params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 64)
          params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false)
          params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 3000)
          params.set(HttpClientUtil.PROP_SO_TIMEOUT, TimeUnit.MINUTES.toMillis(2) + "")
          val httpClient = HttpClientUtil.createClient(params)
          val client = new HttpSolrClient(slice.getLeader.getCoreUrl, httpClient)
          (slice.getName, client)
        }).toMap
        ClientPool.setClients(pool)
        ClientPool.setDateSlices(new DateSlices(docCollection))
      }
    }
    dateSlices
  }
}
