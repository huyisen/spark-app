package com.github.huyisen.spark.app.sink

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-08-29 23:11
  * <p>Version: 1.0
  */
trait Sink extends Serializable {

  def sink()

}
