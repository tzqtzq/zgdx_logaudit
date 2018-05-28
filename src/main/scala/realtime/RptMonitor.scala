package realtime

import java.sql.{Timestamp, Connection}
import java.text.SimpleDateFormat
import java.util.Date

import batchlevel.ESOffline
import com.typesafe.config.ConfigFactory
import contains.Constants
import entry.HDFS
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.commons.logging.{LogFactory, Log}
import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs
import toolpool.{DBManager, SimpleDT}
import tools.{JedisPools, GetConfig, Data2entry}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level

/**
  * 实时处理底层组件日志
  */

// case class CmccOffSet21(consumer:String, topic: String, partition: Int, offset: Long)

object RptMonitor extends Logging{

  val NUMDSSTREAMS = 5

  object SQLContextSingleton {
    @transient  private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }


    def main(args: Array[String]): Unit = {


        // 解析配置文件，获取程序参数
        val load = ConfigFactory.load()
        // kafka集群节点
        val brokers = load.getString("kafka.brokers")
        // 数据所在主题
        val topics = load.getString("kafka.topics")
        // 消费组
        val consumer = load.getString("kafka.consumer")

        // kafka参数
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> brokers, // metadata.broker.list
            "group.id" -> consumer,
//            "auto.create.topics.enable" -> "true",
            "auto.offset.reset" -> "largest" //smallest
        )


       val sparkConf: SparkConf = GetConfig.getsparkConfig(Constants.SIZE_REALTIME)

        // 实时流失处理上下文，批次时间为2秒
        val ssc = new StreamingContext(sparkConf, Seconds(10))

//        ssc.checkpoint("C:\\Users\\tianzhongqiu\\Desktop\\zgdx\\audit")
        // 从kafka中获取数据

        // 程序第一次启动和菲第一次启动两种情况

        // 判断mysql中是否有之前维护过的偏移量信息
        DBs.setup()
        val dbOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
//          var topicAndPartitionList: List[(TopicAndPartition, Long)]= null
//          for (topic <- topics){
            sql"select * from hdfs_offset_test where consumer=? and topic=?"
              .bind(consumer, topics)
              .map(rs =>
                // rs.asInstanceOf[CmccOffSet21]
                // CmccOffSet21(rs.string("consumer"), rs.string("topic"), rs.int("partition_num"), rs.long("offsets"))
                (TopicAndPartition(rs.string("topic"), rs.int("partition_num")), rs.long("offsets"))
              ).list().apply()

//            map.size match {
//              case 0 => topicAndPartitionList
//              case _=> topicAndPartitionList= topicAndPartitionList++map
//            }
//          }
//          topicAndPartitionList
        }.toMap


        val dstream: InputDStream[(String, String)] = if (dbOffsets.size == 0) {
          // 程序第一次启动
          logger.info("-------------- 程序第一次启动 --------------")
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics))

        } else { // 程序非第一次启动

          logger.info("-------------- 程序fei第一次启动 比较偏移量 --------------")

            // 校验我自己维护的偏移量数据和kafka集群中当前最早有效的偏移量谁大谁小，谁大用谁
            val kc = new KafkaCluster(kafkaParams)

            // 主题下的每个分区最早的有效偏移量数据
            val earliestTopicAndPartitionOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getEarliestLeaderOffsets(dbOffsets.keySet)


            val topicAndPartitionToOffset: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestTopicAndPartitionOffsets.right.get

            val currentOffsets = dbOffsets.map(dbs => {
                // 集群中持有的最早有效偏移量数据
                val kcOffsets = topicAndPartitionToOffset.get(dbs._1).get.offset
                if (dbs._2 < kcOffsets) (dbs._1, kcOffsets)
                else dbs
            })

            val messageHandler = (mssg: MessageAndMetadata[String, String]) => (mssg.key(), mssg.message())
            // 根据偏移量从kafka中消费数据
            KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, currentOffsets, messageHandler)
        }




      dstream.foreachRDD(rdd=>{



        if (!rdd.isEmpty()) {

          //获取偏移量范围
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //测试代码
          println(rdd.collect().toBuffer.toString())

          //正式代码
//          val sdc: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
          val sdc: SQLContext = SQLContextSingleton.getInstance(rdd.sparkContext)

          val hdfs: RDD[HDFS] = Data2entry.transHDFS(rdd.map(_._2))

          import sdc.implicits._

          val HDFSDF: DataFrame = hdfs.toDF()

          //-------------------------- 将原始数据存入ES5.2.0---------------------------
          //获取type
          val typeName: String = new SimpleDateFormat("yyyyMMdd").format(new Date())

          logger.info("原始数据数据存入"+Constants.INDEX_HDFS+"/"+typeName)
          EsSpark.saveToEs(hdfs,Constants.INDEX_HDFS+"/"+typeName)

          logger.info("原始数据数据存入ES完毕、、、、、、、、、、、将10秒批次数据写入redis短期存储")

//          JedisPools.getJedis()

          // 更新偏移量信息到MySQL
          offsetRanges.foreach(osr => {

            DB.localTx { implicit session =>
              sql"update hdfs_offset_test SET offsets=?  WHERE consumer=? AND topic=? AND partition_num=? ".bind( osr.untilOffset,consumer, osr.topic,osr.partition).update().apply()
            }

          })

          logger.info("偏移量更新完毕....")

        }

      })

        ssc.start()
        ssc.awaitTermination()
    }


}
