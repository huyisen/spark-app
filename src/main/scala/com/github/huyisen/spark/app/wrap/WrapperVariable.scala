package com.github.huyisen.spark.app.wrap

import scala.reflect.ClassTag

/**
  * 封装不可序列化的对象。
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-09-07 00:31
  * <p>Version: 1.0
  */
class WrapperVariable[T: ClassTag](constructor: => T) extends AnyRef with Serializable {

  @transient private lazy val instance: T = constructor

  def get = instance

}

object WrapperVariable {

  def apply[T: ClassTag](constructor: => T): WrapperVariable[T] = new WrapperVariable[T](constructor)

}
