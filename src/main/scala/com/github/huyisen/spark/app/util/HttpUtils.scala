package com.github.huyisen.spark.app.util

import java.io.IOException

import org.apache.http.NameValuePair
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ByteArrayEntity, ContentType}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.Logging

//import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


/**
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-06-10 23:13
  * <p>Version: 1.0
  */
object HttpUtils extends Logging {


  def post(url: String, parameter: mutable.Map[String, String]): String = {

    val client = clientBuild()

    val parameters = ArrayBuffer[NameValuePair]()
    parameter.foreach(tuple => parameters += new BasicNameValuePair(tuple._1, tuple._2))

    val post = new HttpPost(url)
    post.setConfig(RequestConfig.DEFAULT)
    post.setEntity(new UrlEncodedFormEntity(parameters, Constants.CHARSET_NAME))

    execute(client, post)
  }

  def post(url: String, data: Array[Byte]): String = {
    val client = clientBuild()

    val post = new HttpPost(url)
    post.setConfig(RequestConfig.DEFAULT)
    post.setEntity(new ByteArrayEntity(data, ContentType.APPLICATION_JSON))
    logDebug(s"url=$url, parameters=$data")
    execute(client, post)
  }

  def clientBuild(userAgent: String = "Default-Client"): CloseableHttpClient = {
    HttpClientBuilder.create().setUserAgent(userAgent).build()
  }

  def execute(client: CloseableHttpClient, post: HttpPost): String = {
    val response = client.execute(post)
    val respEntity = response.getEntity
    val statusCode = response.getStatusLine.getStatusCode

    if (statusCode != 200) {
      logError(s"Server error statusCode = $statusCode")
      throw new IOException(s"Server error statusCode = $statusCode")
    }

    if (respEntity == null) {
      logInfo(s"Server return nothing!")
      throw new IOException("Server return nothing!")
    }

    EntityUtils.toString(respEntity, Constants.CHARSET_NAME)
  }

}
