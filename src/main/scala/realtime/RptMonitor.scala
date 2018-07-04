package realtime

import java.sql.{Timestamp, Connection}
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import contains.Constants
import entry.{YARN, HIVE, HDFS}
import jdbc.JDBCHelper
import kafka.common.TopicAndPartition

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.logging.{LogFactory, Log}
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.{LeaderOffset, Err}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import realtime.KafkaDSream.SQLContextSingleton
import scalikejdbc._
import scalikejdbc.config.DBs
import toolpool.{DBManager, SimpleDT}
import tools.{StopSCCWithElegant, GetConfig, Data2entry}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark

/**
  * 实时处理底层组件日志
  */

// case class CmccOffSet21(consumer:String, topic: String, partition: Int, offset: Long)

object RptMonitor {


    def main(args: Array[String]): Unit = {


        // 解析配置文件，获取程序参数
        val load = ConfigFactory.load()
        // 数据所在主题
         val topics = load.getString("kafka.topics").split(" ")
        // 消费组
        val consumer = load.getString("kafka.consumer")
        val brokers = load.getString("kafka.brokers")


        //kafka集群信息配置
         val kafkaParams = Map[String, String](
        "bootstrap.servers" -> brokers, // metadata.broker.list
        "group.id" -> consumer,
        //            "auto.create.topics.enable" -> "true",
        "auto.offset.reset" -> "largest" //smallest
      )

       val sparkConf: SparkConf = GetConfig.getsparkConfig(Constants.SIZE_REALTIME,"yarn")

        // 实时流失处理上下文，批次时间为10秒
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))

      // 判断mysql中是否有之前维护过的偏移量信息
        DBs.setup()

      val dbOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
        var topicAndPartitionList: Map[TopicAndPartition, Long] =Map()
        for (topicName <- topics) {
          topicAndPartitionList=topicAndPartitionList.++(sql"select * from hdfs_offset_test where consumer=? and topic=?"
            .bind(consumer, topicName)
            .map(rs =>
              (TopicAndPartition(rs.string("topic"), rs.int("partition_num")), rs.long("offsets"))
            ).list().apply().toMap)
        }
        println(topicAndPartitionList.toBuffer.toString())
        topicAndPartitionList
      }


      //方法获取数据流，存入ES和redis中
      KafkaDSream.getStream(dbOffsets,ssc)
//        val kc = new KafkaCluster(kafkaParams)

        ssc.start()
        ssc.awaitTermination()
      //优雅的停止数据流
//      StopSCCWithElegant.stopByMarkFile(ssc)
    }


}
