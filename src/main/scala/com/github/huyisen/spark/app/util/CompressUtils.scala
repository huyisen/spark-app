package com.github.huyisen.spark.app.util

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }

/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-11 14:54
  * <p>Version: 1.0
  */
object CompressUtils {

  def compress(src: String): Array[Byte] = {
    if (src == null || src.length == 0) return null
    val out = new ByteArrayOutputStream
    val gzip = new GZIPOutputStream(out)
    gzip.write(src.getBytes(Constants.CHARSET_NAME))
    gzip.close()
    out.toByteArray
  }

  // 解压缩
  def uncompress(src: Array[Byte]): Array[Byte] = {
    if (src == null || src.length == 0) return src
    val out = new ByteArrayOutputStream
    val in = new ByteArrayInputStream(src)
    val gunzip = new GZIPInputStream(in)
    val buffer = new Array[Byte](512)

    var len = 0
    while ( {
      len = gunzip.read(buffer)
      len != -1
    }) {
      out.write(buffer, 0, len)
    }
    out.toByteArray
  }

  def main(args: Array[String]): Unit = {
    val str = "我是中国程序员"
    val com = compress(str)
    println(new String(com, Constants.CHARSET_NAME))
    val uncom = uncompress(com)
    println(new String(uncom, Constants.CHARSET_NAME))

  }

}
