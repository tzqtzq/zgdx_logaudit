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
import realtime.KafkaDSream.SQLContextSingleton
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

      val kafkaParams = Map[String, String](
        "bootstrap.servers" -> brokers, // metadata.broker.list
        "group.id" -> consumer,
        //            "auto.create.topics.enable" -> "true",
//        "auto.offset.reset" -> "largest" //smallest
        "auto.offset.reset" -> "smallest" //smallest
      )

       val sparkConf: SparkConf = GetConfig.getsparkConfig(Constants.SIZE_REALTIME)

        // 实时流失处理上下文，批次时间为2秒
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))
//      ssc.checkpoint("")

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



      KafkaDSream.getStream(dbOffsets,ssc)
//val kc = new KafkaCluster(kafkaParams)

//      val a :InputDStream[(String,String)]=if (dbOffsets.size == 0) {
//        // 程序第一次启动
//        println("-------------- 程序第一次启动 --------------")
//      val a= KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(Constants.TOPIC_HDFS))


//      } else { // 程序非第一次启动
//
//        println("-------------- 程序fei第一次启动 比较偏移量 --------------")
//
//        // 校验我自己维护的偏移量数据和kafka集群中当前最早有效的偏移量谁大谁小，谁大用谁
//        // 主题下的每个分区最早的有效偏移量数据
//
//        var hdfsKeys: Map[TopicAndPartition, Long] = dbOffsets.filterKeys(_.topic.equals(Constants.TOPIC_HDFS))
//        println(hdfsKeys.keySet.toBuffer.toString())
//        val earliestTopicAndPartitionOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getEarliestLeaderOffsets(dbOffsets.keySet)

////        val topicAndPartitionToOffset = earliestTopicAndPartitionOffsets.left.get
//        val topicAndPartitionToOffset = earliestTopicAndPartitionOffsets.right.get
//
//        println(earliestTopicAndPartitionOffsets.right.get)
//
////
//        val currentOffsets = dbOffsets.map(dbs => {
//          // 集群中持有的最早有效偏移量数据
//          val kcOffsets = topicAndPartitionToOffset.get(dbs._1).get.offset
//          if (dbs._2 < kcOffsets) (dbs._1, kcOffsets)
//          else dbs
//        })
//
//        import kafka.message.MessageAndMetadata
//
//        val messageHandler = (mssg: MessageAndMetadata[String, String]) => (mssg.key(), mssg.message())
//        // 根据偏移量从kafka中消费数据
//        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, currentOffsets, messageHandler)
//      }

//      a.foreachRDD(rdd=>{
//
//        rdd.foreach(println)
//      })
        ssc.start()
        ssc.awaitTermination()
      //优雅的停止数据流
//      StopSCCWithElegant.stopByMarkFile(ssc)
    }


}
