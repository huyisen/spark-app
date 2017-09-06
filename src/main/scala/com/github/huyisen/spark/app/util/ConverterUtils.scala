package com.github.huyisen.spark.app.util

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-01 22:45
  * <p>Version: 1.0
  */
object ConverterUtils {

val family = Bytes.toBytes("c")

  /**
    * <p>将 HBase 中的查询结果 Result 换成 SolrInputDocument。
    * <p> Qualifier中的ID为SolrInputDocument重的ID，
    * <p>若Qualifier中没有ID则那第一个cell 的 rowKey。
    *
    * @param result HBase的Result
    * @param fields solr中schema中的fields
    * @return SolrInputDocument
    */
  def result2Document(result: Result, fields: Set[String]): SolrInputDocument = {
    map2Document(result2Map(result), fields)
  }

  /**
    * 将 Map 对象转成 SolrInputDocument.
    *
    * @param map    Map 对象， map 中必须含有 "ID"
    * @param fields solr中schema中的fields
    * @return SolrInputDocument
    */
  def map2Document(map: Map[String, AnyRef], fields: Set[String]): SolrInputDocument = {
    val doc = new SolrInputDocument

    if (!map.contains("ID")) {
      throw new IllegalArgumentException("Map 对象， map 中必须含有 ID ")
    }

    map.foreach(tuple => {
      val key = tuple._1
      var value = tuple._2

      if (fields.contains(key)) {
        if ("null".equals(String.valueOf(value)))
          value = ""

        doc.setField(key, value)
      }
    })
    doc
  }

  /**
    * 将 HBase Result 转 Map 对象。
    *
    * @param result HBase Result
    * @return Map 对象
    */
  def result2Map(result: Result): Map[String, AnyRef] = {
    val map: Map[String, AnyRef] = Map() //初始化构造函数

    val cells = result.listCells()
    if (cells == null || cells.isEmpty)
      return map

    for (cell <- cells.asScala) {
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      map += (key -> value)
    }

    map.get("ID") match {
      case None => map += ("ID" -> Bytes.toString(CellUtil.cloneRow(cells.get(0))))
      case _ => //do nothing
    }
    map
  }

  /**
    *  map 转 HBase Put
    *
    * @param map map对象
    * @return Put 对象
    */
  def map2Put(map: mutable.Map[String, AnyRef]): Put = {
    if (!map.contains("ID")) {
      throw new IllegalArgumentException("Map 对象， map 中必须含有 ID ")
    }

    val put = new Put(Bytes.toBytes(map("ID").toString))
    val current = System.currentTimeMillis()
    map.foreach(tuple =>
      put.addColumn(family, Bytes.toBytes(tuple._1), current, Bytes.toBytes(tuple._2.toString))
    )
    put
  }

  def scan2String(scan: Scan): String = {
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

}
