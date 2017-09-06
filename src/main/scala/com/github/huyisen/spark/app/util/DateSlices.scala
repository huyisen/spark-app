package com.github.huyisen.spark.app.util

import java.util.Comparator

import org.apache.solr.common.cloud.{CompositeIdRouter, DocCollection, DocRouter, Slice}
import org.apache.solr.common.util.Hash
import com.google.common.collect.Range

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-17 00:06
  * <p>Version: 1.0
  */
class DateSlices(docCollection: DocCollection) extends Serializable {
  val collections = mutable.HashMap[String, CollectionSlices]()

  val activeSlices: Iterable[Slice] = docCollection.getActiveSlices
  for (s <- activeSlices) {
    val name = toCollectionName(s.getName)
    val cs = collections.getOrElseUpdate(name, new CollectionSlices(name))
    cs.slices.add(s)
  }
  collections.values.foreach(_.partition())
  val dates = collections.keySet.map(key => new Integer(key.toInt)).toList.sortWith(_.compareTo(_) < 0)
  for (i <- 1 to dates.size) {
    val range = if (i < dates.size) Range.closedOpen(new Integer(dates(i - 1)), new Integer(dates(i)))
    else Range.atLeast(new Integer(dates(i - 1)))

    collections.keys
      .map(_.toInt)
      .filter(range.contains(_))
      .foreach(v => collections(String.valueOf(v)).dateRange = range)
  }

  def toCollectionName(sliceName: String): String = sliceName.substring(0, sliceName.indexOf(Constants.DIVIDED))

  /**
    * @param rowKey value of route field
    * @param time   fromat yyyyMMdd
    * @return
    */
  def getRoute(rowKey: String, time: Int): String = {
    for (coll <- collections.values) {
      if (coll.isCollection(time)) return coll.isShard(rowKey)
    }
    collections(Constants.TIMEERROR).isShard(rowKey)
  }

  class CollectionSlices(name: String) extends Serializable {

    private[app] val slices = new ArrayBuffer[Slice]()
    private[app] var dateRange: Range[Integer] = _ // [yyyyMMdd1-yyyyMMdd)
    private val sliceRanges = new ArrayBuffer[SliceRang]

    def partition(): Unit = {
      slices.sort(new Comparator[Slice] {
        override def compare(slice1: Slice, slice2: Slice): Int = {
          val name1 = slice1.getName
          val name2 = slice2.getName
          val s1 = name1.substring(name1.indexOf(Constants.DIVIDED) + 1, name1.length).toInt
          val s2 = name2.substring(name2.indexOf(Constants.DIVIDED) + 1, name2.length).toInt
          s1.compareTo(s2)
        }
      })
      val ranges = new CompositeIdRouter().partitionRange(slices.size, new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE))
      for (i <- 0 until ranges.size()) {
        sliceRanges.add(new SliceRang(slices.get(i), ranges.get(i)))
      }
    }

    def isCollection(time: Int): Boolean = dateRange.contains(new Integer(time))

    def isShard(rowKey: String): String = {
      val hash = Hash.murmurhash3_x86_32(rowKey, 0, rowKey.length, 0)
      for (shard <- sliceRanges if shard.range.includes(hash)) {
        return shard.slice.getName
      }
      null
    }

    private[app] class SliceRang(var slice: Slice, var range: DocRouter.Range) extends Serializable

  }

}

