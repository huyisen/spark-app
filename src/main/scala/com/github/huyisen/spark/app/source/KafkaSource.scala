package com.github.huyisen.spark.app.source

import com.github.huyisen.spark.app.util.KafkaCluster
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
  *
  *
  *
  * <p>Author: huyisen@gmail.com
  * <p>Date: 2017-07-13 00:40
  * <p>Version: 1.0
  */
class KafkaSource[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
(val kafkaParams: Map[String, String]) extends Serializable {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val kc = new KafkaCluster(kafkaParams)

  def createDStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams("group.id")
    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)
    //从zookeeper上读取offset开始消费message

    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
        throw new SparkException("get kafka partition failed:")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft)
        throw new SparkException("get kafka consumer offsets failed:")
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    }
    messages
  }


  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics
    * @param groupId
    */
  private[app] def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) throw new SparkException(s"get kafka partition failed: ${ partitionsE.left.get.mkString("\n") }")
      val partitions: Set[TopicAndPartition] = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      val hasConsumed = if (consumerOffsetsE.isLeft) false else true
      logger.info("consumerOffsetsE.isLeft: " + consumerOffsetsE.isLeft)
      if (hasConsumed) {
        // 消费过
        logger.warn("消费过")
        //如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
        // 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
        //如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
        //这时把consumerOffsets更新为earliestLeaderOffsets
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest offsets failed: ${ earliestLeaderOffsetsE.left.get.mkString("\n") }")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({
          case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              logger.warn("consumer group:" + groupId + ",topic:" + tp.topic +
                ",partition:" + tp.partition + " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += (tp -> earliestLeaderOffset)
            }
        })
        logger.warn("offsets: " + consumerOffsets)
        if (offsets.nonEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {
        // 没有消费过
        logger.warn("没消费过")
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        val leaderOffsets = if (reset == Some("smallest")) kc.getEarliestLeaderOffsets(partitions).right.get
        else kc.getLatestLeaderOffsets(partitions).right.get

        val offsets = leaderOffsets.map{ case (tp, offset) => (tp, offset.offset) }
        logger.warn("offsets: " + offsets)
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  /**
    * 更新zookeeper上的消费offsets
    *
    * @param rdd
    */
  private[app] def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams("group.id")
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        logger.error(s"Error updating the offset to Kafka cluster: ${ o.left.get }")
      }
    }
  }

}
