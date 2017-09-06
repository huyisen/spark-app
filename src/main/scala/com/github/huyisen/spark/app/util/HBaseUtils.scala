package com.github.huyisen.spark.app.util

import com.github.huyisen.spark.app.dto.{Dto, MapDto}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-10 23:48
  * <p>Version: 1.0
  */
object HBaseUtils {


  def put(tableName: String, quorum: String, puts: mutable.Buffer[Put], parent: String = Constants.NODE_PARENT
         ): Unit = {
    val connection = createConnection(tableName, quorum, parent)
    val table = connection.getTable(TableName.valueOf(tableName))
    table.put(puts.asJava)
    table.close()
    connection.close()
  }

  def get(tableName: String, quorum: String, rowKeys: mutable.Buffer[String], parent: String = Constants.NODE_PARENT
         ): mutable.Buffer[MapDto] = {
    val connection = createConnection(tableName, quorum, parent)
    val table = connection.getTable(TableName.valueOf(tableName))
    val result = new mutable.ArrayBuffer[MapDto]()
    val gets = rowKeys.map(rowKey => new Get(Bytes.toBytes(rowKey)))
    val rss = table.get(gets.asJava)
    for (rs <- rss) {
      result += Dto.result2MapDto(rs)
    }
    table.close()
    connection.close()
    result
  }

  def createConnection(tableName: String, quorum: String, parent: String): Connection = {
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("zookeeper.znode.parent", parent)
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.client.pause", "2000")
    conf.set("hbase.client.retries.number", "50")
    conf.set("hbase.rpc.timeout", "120000")
    conf.set("hbase.client.operation.timeout", "120000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    ConnectionFactory.createConnection(conf)
  }


}
