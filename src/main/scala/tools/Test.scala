package tools

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.typesafe.config.ConfigFactory
import contains.Constants
import entry.HIVE
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import scalikejdbc.DB
import scalikejdbc.config.DBs
import scalikejdbc._
import scalikejdbc.config.DBs
import toolpool.SimpleDT

import scala.collection.mutable

/**
  * Created by tianzhongqiu on 2018/5/8.
  */


object Test{



  def main(args: Array[String]) {

    val load = ConfigFactory.load()
    // 数据所在主题
    val topics = load.getString("kafka.topics").split(" ")
    println(topics.length)
    // 消费组
    val consumer = load.getString("kafka.consumer")
    val brokers = load.getString("kafka.brokers")


    DBs.setup()

    val dbOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      var topicAndPartitionList: Map[TopicAndPartition, Long] =Map()
      for (topicName <- topics) {
        topicAndPartitionList=topicAndPartitionList++(sql"select * from hdfs_offset_test where consumer=? and topic=?"
        .bind(consumer, topicName)
        .map(rs =>
          (TopicAndPartition(rs.string("topic"), rs.int("partition_num")), rs.long("offsets"))
        ).list().apply().toMap)
            }
      topicAndPartitionList
          }

    val hdfsKeys: Map[TopicAndPartition, Long] = dbOffsets.filterKeys(_.topic.equals(Constants.TOPIC_HDFS))

    printf(hdfsKeys.keySet.toBuffer.toString())
//
//    println(dbOffsets.toBuffer.toString())

//    val a: Array[OffsetRange] =Array(OffsetRange("logauditHIVE",1,15,255),OffsetRange("logauditHIVE",2,15,255),OffsetRange("logauditHIVE",0,15,255),OffsetRange("logauditHDFS",0,15,223)
//    ,OffsetRange("logauditHDFS",2,15,223),OffsetRange("logauditHDFS",1,15,223))
//



//    updateNewOff(a,"csm_20180508_03")

  }

  def updateNewOff(offsetRanges: Array[OffsetRange],consumer:String): Unit = {

    offsetRanges.foreach(osr => {

      DB.localTx { implicit session =>
        sql"update hdfs_offset_test SET offsets=?  WHERE consumer=? AND topic=? AND partition_num=? ".bind(osr.untilOffset, consumer, osr.topic, osr.partition).update().apply()
      }

    })
  }

}
