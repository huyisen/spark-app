package com.github.huyisen.spark.app.source

import java.util.concurrent.TimeUnit

import com.github.huyisen.spark.app.util.ConverterUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat, Job => NewHadoopJob }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-06 17:36
  * <p>Version: 1.0
  */
class HBaseSource
  extends Serializable {

  def createRDD(
    sc: SparkContext,
    conf: Configuration
  ): RDD[(ImmutableBytesWritable, Result)] = {

    sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

  }

}

object HBaseSource {


  def defaultConf(
    quorum: String,
    tableName: String,
    scan: Scan = defaultScan()
  ): Configuration = {

    val conf = HBaseConfiguration.create

    conf.set("hbase.client.pause", "2000")
    conf.set("hbase.rpc.timeout", "120000")
    conf.set("hbase.client.retries.number", "50")
    conf.set("hbase.client.operation.timeout", "120000")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.client.scanner.timeout.period", String.valueOf(TimeUnit.HOURS.toMillis(3)))
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN, ConverterUtils.scan2String(scan))

    conf
  }

  def defaultScan(
    minStamp: Long = 0L,
    maxStamp: Long = 0L,
    families: String = "c",
    caching: Int = 100,
    batch: Int = 1000
  ): Scan = {

    val scan = new Scan()
    if (minStamp > 0L && maxStamp > 0L) {
      scan.setTimeRange(minStamp, maxStamp)
    }

    if (families.contains(",")) {
      val fs = families.split(",")
      for (f <- fs) {
        scan.addFamily(Bytes.toBytes(f))
      }
    } else {
      scan.addFamily(Bytes.toBytes(families))
    }

    scan.setCacheBlocks(false) //cacheBlocks must set false
      .setCaching(caching)
      .setBatch(batch)
  }

}