package com.github.huyisen.spark.app.util

import java.util.concurrent.TimeUnit

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-07 00:59
  * <p>Version: 1.0
  */
object TimeUtils {

  def millis2ddHHmmSS(millis: Long): String = {

    val day = millis / TimeUnit.DAYS.toMillis(1)
    val hour = (millis - TimeUnit.DAYS.toMillis(day)) / TimeUnit.HOURS.toMillis(1)
    val minute = (millis - TimeUnit.DAYS.toMillis(day) - TimeUnit.HOURS.toMillis(hour)) / TimeUnit.MINUTES.toMillis(1)
    val second = (millis - TimeUnit.DAYS.toMillis(day) - TimeUnit.HOURS.toMillis(hour) - TimeUnit.MINUTES.toMillis(minute)) / TimeUnit.SECONDS.toMillis(1)
    val milli = millis - TimeUnit.DAYS.toMillis(day) - TimeUnit.HOURS.toMillis(hour) - TimeUnit.MINUTES.toMillis(minute) - TimeUnit.SECONDS.toMillis(second)

    "%02d:%02d:%02d.%03d".format(hour, minute,second, milli)
  }

}
