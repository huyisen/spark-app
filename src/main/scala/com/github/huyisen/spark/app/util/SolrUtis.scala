package com.github.huyisen.spark.app.util

import java.io.IOException

import com.github.huyisen.spark.app.dto.{Dto, MapDto}
import org.apache.hadoop.hbase.util.Strings
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.schema.SchemaRequest
import org.apache.solr.client.solrj.response.schema.SchemaResponse
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud.Slice
import org.apache.solr.common.util.Hash
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-08 22:02
  * <p>Version: 1.0
  */
object SolrUtils extends Logging {


  def sinkMapDto(clientPool: Map[String, CloudSolrClient], fields: Set[String],
                 slices: Iterable[Slice], iterator: Iterator[MapDto]): Unit = {
    val docSize = 3
    val clientsDocList = clientPool.keys.map(shard => (shard, new ArrayBuffer[SolrInputDocument](docSize))).toMap //shard ->docList

    val start = System.currentTimeMillis()
    while (iterator.hasNext) {

      val mapDto = iterator.next()
      val solrInputDocument = Dto.mapDto2Document(mapDto, fields)
      val shard = getShardById(mapDto.key, slices)
      val docList = clientsDocList(shard)
      docList += solrInputDocument
      if (docList.length % docSize == 0) try {
        clientPool(shard).add(docList)
        docList.clear()
      } catch {
        case sse: SolrServerException => logError("SolrServerException ", sse)
        case ie: IOException => logError("IOException ", ie)
        case e: Exception => logError("Exception", e)
      }

    }

    clientsDocList.foreach(tuple => {
      val docs = tuple._2
      val client = clientPool(tuple._1)
      if (docs.nonEmpty) try {
        client.add(docs)
      } catch {
        case sse: SolrServerException => logError("SolrServerException ", sse)
        case ie: IOException => logError("IOException ", ie)
        case e: Exception => logError("Exception", e)
      }
      client.commit()
    })

    val end = System.currentTimeMillis()
    val cost = end - start
    logWarning(String.format("Consumer cost %s", TimeUtils.millis2ddHHmmSS(cost)))

  }


  def getFields(client: CloudSolrClient, collection: String = null): Set[String] = {
    val request = new SchemaRequest
    val response = new SchemaResponse

    if (!Strings.isEmpty(collection))
      client.setDefaultCollection(collection)

    response.setResponse(client.request(request))
    val schema = response.getSchemaRepresentation
    schema.getFields
      .map(f => f.get("name").toString)
      .filter(field => !field.contains("_") || "_route_".equals(field))
      .toSet
  }

  def getSlices(client: CloudSolrClient, collection: String = null): Iterable[Slice] = {
    if (!Strings.isEmpty(collection))
      client.setDefaultCollection(collection)

    client.getZkStateReader.getClusterState.getCollection(collection).getSlices
  }

  def getShardById(id: String, slices: Iterable[Slice]): String = {
    val idHash = Hash.murmurhash3_x86_32(id, 0, id.length, 0)
    for (slice <- slices) {
      val range = slice.getRange
      if (range != null && range.includes(idHash)) return slice.getName
    }
    import org.apache.solr.common.SolrException
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No slice servicing hash code " + Integer.toHexString(idHash) + " in " + slices)
  }

  def sinkMapDto(collection: String, zkHost: String, fields: Set[String], iterator: Iterator[MapDto]): Unit = {
    val cloudClient = new CloudSolrClient(zkHost)
    cloudClient.setDefaultCollection(collection)
    cloudClient.connect()
    val zkReader = cloudClient.getZkStateReader
    val slices = zkReader.getClusterState.getCollection(collection).getSlices
    cloudClient.close()

    val docSize = 10000

    val clients = slices.map(slice => {
      val client = new CloudSolrClient(zkHost)
      client.setDefaultCollection(collection)
      client.connect()
      (slice.getName, client)
    }).toMap // shard -> HttpSolrClient

    val clientsDocList = clients.keys.map(shard => (shard, new ArrayBuffer[SolrInputDocument](docSize))).toMap //shard ->docList

    val start = System.currentTimeMillis()
    while (iterator.hasNext) {

      val mapDto = iterator.next()
      val solrInputDocument = Dto.mapDto2Document(mapDto, fields)
      val shard = getShardById(mapDto.key, slices)
      val docList = clientsDocList(shard)
      docList += solrInputDocument
      if (docList.length % docSize == 0) try {
        clients(shard).add(docList)
        docList.clear()
      } catch {
        case sse: SolrServerException => logError("SolrServerException ", sse)
        case ie: IOException => logError("IOException ", ie)
        case e: Exception => logError("Exception", e)
      }

    }

    clientsDocList.foreach(tuple => {
      val docs = tuple._2
      val client = clients(tuple._1)
      if (docs.nonEmpty) try {
        client.add(docs)
      } catch {
        case sse: SolrServerException => logError("SolrServerException ", sse)
        case ie: IOException => logError("IOException ", ie)
        case e: Exception => logError("Exception", e)
      }
      client.commit()
      client.close()
    })

    val end = System.currentTimeMillis()
    val cost = end - start
    logWarning(String.format("Consumer cost %s", TimeUtils.millis2ddHHmmSS(cost)))

  }


}
