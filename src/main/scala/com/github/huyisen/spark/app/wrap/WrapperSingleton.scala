package com.github.huyisen.spark.app.wrap

import java.util.UUID

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 00:30
  * <p>Version: 1.0
  */
class WrapperSingleton[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
  val singletonUUID = UUID.randomUUID().toString

  @transient private lazy val instance: T = {

    WrapperSingleton.singletonPool.synchronized {
      val singletonOption = WrapperSingleton.singletonPool.get(singletonUUID)
      if (singletonOption.isEmpty) {
        WrapperSingleton.singletonPool.put(singletonUUID, constructor)
      }
    }

    WrapperSingleton.singletonPool.get(singletonUUID).get.asInstanceOf[T]
  }

  def get = instance

}

object WrapperSingleton {

  private val singletonPool = new TrieMap[String, Any]()

  def apply[T: ClassTag](constructor: => T): WrapperSingleton[T] = new WrapperSingleton[T](constructor)

  def poolSize: Int = singletonPool.size

  def poolClear(): Unit = singletonPool.clear()

}
