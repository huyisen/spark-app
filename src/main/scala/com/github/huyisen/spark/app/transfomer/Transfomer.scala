package com.github.huyisen.spark.app.transfomer

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-13 23:01
  * <p>Version: 1.0
  */
object Transform {


  val fc = Bytes.toBytes("c")

  val defaultID = "1234567890"

  val kafkaToHBase = (src: (String, String)) => {
    val obj = JSON.parseObject(src._2).asScala
    val rowKey = obj.getOrElse("ID", defaultID)
    val put = new Put(Bytes.toBytes(String.valueOf(rowKey)))
    for ((k, v) <- obj)
      put.addColumn(fc, Bytes.toBytes(k), System.currentTimeMillis(), Bytes.toBytes(String.valueOf(v)))
    (new ImmutableBytesWritable, put)
  }

  val kafkaToSolr = (src: (String, String)) => {
    val obj = JSON.parseObject(src._2).asScala
    val doc = new SolrInputDocument
    for ((k, v) <- obj) doc.setField(k, v)
    doc.setField("ID", obj.getOrElse("ID", defaultID))
    doc
  }

}
