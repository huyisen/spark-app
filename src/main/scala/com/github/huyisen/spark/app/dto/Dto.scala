package com.github.huyisen.spark.app.dto

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-08 17:22
  * <p>Version: 1.0
  */
abstract class Dto extends Serializable {
  val key: String
  if (key == null || "".equals(key))
    throw new IllegalArgumentException("Dto key can not be null or empty.")

}

case class MapDto(key: String, map: mutable.Map[String, AnyRef]) extends Dto

case class ResultDto(key: String, result: Result) extends Dto

//case class PutDto(key: String, put: Put) extends Dto

object Dto {

  //TODO atomic update
  def mapDto2Document(dto: MapDto, fields: Set[String]): SolrInputDocument = {

    val doc = new SolrInputDocument
    doc.setField("ID", dto.key)

    dto.map.foreach(tuple => {
      val key = tuple._1
      val value = tuple._2
      if (fields.contains(key)) {
        doc.setField(key, if ("null".equals(String.valueOf(value))) "" else value)
      }
    })
    doc
  }

  val family = Bytes.toBytes("c")

  def mapDto2Put(dto: MapDto): Put = {

    val put = new Put(Bytes.toBytes(dto.key))
    val current = System.currentTimeMillis()
    dto.map.foreach(tuple =>
      put.addColumn(family, Bytes.toBytes(tuple._1), current, Bytes.toBytes(tuple._2.toString))
    )

    put
  }

  def result2MapDto(result: Result): MapDto = {
    val map = mutable.Map[String, AnyRef]()
    val cells = result.listCells()
    if (cells == null || cells.isEmpty)
      throw new IllegalArgumentException("Result is null or empty.")

    cells.asScala.map(cell =>
      map += (Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell)))
    )

    val key = if (map.get("ID").isEmpty) Bytes.toString(CellUtil.cloneRow(cells.get(0))) else map("ID").toString

    MapDto(key, map)
  }

  def mapDto2HadoopApiPut(dto: MapDto): (ImmutableBytesWritable, Put) = {
    (new ImmutableBytesWritable, mapDto2Put(dto))
  }

  def hadoopApiResult2MapDto(result: (ImmutableBytesWritable, Result)): MapDto = {
    result2MapDto(result._2)
  }


}