package realtime

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.typesafe.config.ConfigFactory
import contains.Constants
import entry.{YARN, HIVE, HDFS}
import kafka.common.TopicAndPartition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.spark.streaming.kafka.KafkaCluster.{LeaderOffset, Err}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder
import org.elasticsearch.spark.rdd.EsSpark
import org.slf4j.LoggerFactory
import tools.Data2entry

import scala.collection

/**
  * Created by tianzhongqiu on 2018/5/29.
  */
object KafkaDSream {

  val logger = LoggerFactory.getLogger(KafkaDSream.getClass)

  // 解析配置文件，获取程序参数
  val load = ConfigFactory.load()
  // kafka集群节点
  val brokers = load.getString("kafka.brokers")
  // 消费组
  val consumer = load.getString("kafka.consumer")

  // kafka参数
  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> brokers, // metadata.broker.list
    "group.id" -> consumer,
    "serializer.class" -> "kafka.serializer.StringEncoder",
    //            "auto.create.topics.enable" -> "true",
    "auto.offset.reset" -> "largest" //smallest
  )

  object SQLContextSingleton {
    @transient  private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

  def getStream(dbOffsets: Map[TopicAndPartition, Long],ssc: StreamingContext):Unit={

    var hdfsKeys: Map[TopicAndPartition, Long] = dbOffsets.filterKeys(_.topic.equals(Constants.TOPIC_HDFS))
    var hiveKeys: Map[TopicAndPartition, Long] = dbOffsets.filterKeys(_.topic.equals(Constants.TOPIC_HIVE))
    var yarnKeys: Map[TopicAndPartition, Long] = dbOffsets.filterKeys(_.topic.equals(Constants.TOPIC_YARN))

//    val kc = new KafkaCluster(kafkaParams)
//    val earliestTopicAndPartitionOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getLatestLeaderOffsets(hdfsKeys.keySet)
//
//    val topicAndPartitionToOffset: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestTopicAndPartitionOffsets.right.get
//
//    println(topicAndPartitionToOffset.toBuffer.toString())

      //获取每个topic的数据流
//    val hdfsDS: InputDStream[(String, String)] =judgmentOffsets(hdfsKeys,ssc,Constants.TOPIC_HDFS)
//    val hiveDS: InputDStream[(String, String)] = judgmentOffsets(hiveKeys,ssc,Constants.TOPIC_HIVE)
//    val yarnDS: InputDStream[(String, String)] = judgmentOffsets(yarnKeys,ssc,Constants.TOPIC_YARN)

      dealDS(judgmentOffsets(hdfsKeys,ssc,Constants.TOPIC_HDFS),Constants.TOPIC_HDFS)
      dealDS(judgmentOffsets(hiveKeys,ssc,Constants.TOPIC_HIVE),Constants.TOPIC_HIVE)
      dealDS(judgmentOffsets(yarnKeys,ssc,Constants.TOPIC_YARN),Constants.TOPIC_YARN)




  }

  def judgmentOffsets(dbOffsets:Map[TopicAndPartition, Long],ssc: StreamingContext,typeName:String):InputDStream[(String,String)]={

    val kc = new KafkaCluster(kafkaParams)

//    //保证kafka集群的分区没有扩展（topic的分区数据和之前一致）
//    val partitions: Either[Err, Set[TopicAndPartition]] = kc.getPartitions(Set(typeName))
//    val partitionsSize: Array[Int] = partitions.right.get.map(_.partition).toArray
//    var newdbOffsets:Map[TopicAndPartition, Long]=Map()
//
//    if(dbOffsets.size<partitionsSize.length){
//      //得到旧的所有分区序号
//      val old_partitions=dbOffsets.keys.map(p=>p.partition).toArray
//      //通过做差集得出来多的分区数量数组
//      val add_partitions=partitionsSize.diff(old_partitions)
//      if(add_partitions.size>0){
//        logger.warn("发现kafka新增分区："+add_partitions.mkString(","))
//        add_partitions.foreach(partitionId=>{
//          newdbOffsets=dbOffsets
//          newdbOffsets=newdbOffsets.+=(TopicAndPartition(typeName,partitionId)->0)
//          //更新到数据库
//          UpdateOffsets.insertNewOff(Array(OffsetRange(typeName,partitionId,0,0)),consumer)
//          logger.warn("新增分区id："+partitionId+"添加完毕....")
//        })
//
//      }
//
//    }else{
//      logger.warn("没有发现新增的kafka分区："+partitionsSize.mkString(","))
//    }

//    if (dbOffsets.size == 0) {
    // 程序第一次启动
    println("-------------- 程序第一次启动 --------------")
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(typeName))

//  } else { // 程序非第一次启动
//
//    println("-------------- 程序fei第一次启动 比较偏移量 --------------")
//
//    // 校验我自己维护的偏移量数据和kafka集群中当前最早有效的偏移量谁大谁小，谁大用谁
//    // 主题下的每个分区最早的有效偏移量数据
//    val earliestTopicAndPartitionOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kc.getLatestLeaderOffsets(dbOffsets.keySet)
//
//    val topicAndPartitionToOffset: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestTopicAndPartitionOffsets.right.get
//
//
//
//    val currentOffsets = dbOffsets.map(dbs => {
//    // 集群中持有的最早有效偏移量数据
//    val kcOffsets = topicAndPartitionToOffset.get(dbs._1).get.offset
//    if (dbs._2 < kcOffsets) (dbs._1, kcOffsets)
//    else dbs
//  })
//
//      import kafka.message.MessageAndMetadata
//
//    val messageHandler = (mssg: MessageAndMetadata[String, String]) => (mssg.key(), mssg.message())
//    // 根据偏移量从kafka中消费数据
//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, currentOffsets, messageHandler)
//  }

  }

  def dealDS(dsStream: InputDStream[(String, String)] ,name:String): Unit = {

    dsStream.foreachRDD(rdd => {

      if (!rdd.isEmpty()) {

        //获取偏移量范围
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //正式代码
        //          val sdc: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        val sdc: SQLContext = SQLContextSingleton.getInstance(rdd.sparkContext)

        //-------------------------- 将原始数据存入ES5.2.0---------------------------
        //获取type
        val typeName: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        val yesTypeName: String = getYesterday

        name match {

          case Constants.TOPIC_HDFS => val hdfs: RDD[HDFS] = Data2entry.transHDFS(rdd.map(_._2))
            println("原始数据数据存入" + Constants.INDEX_HDFS + "/" + hdfs)
            EsSpark.saveToEs(hdfs.filter(_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + typeName)
            EsSpark.saveToEs(hdfs.filter(!_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + yesTypeName)


          case Constants.TOPIC_HDFS =>val hdfs: RDD[HDFS] = Data2entry.transHDFS(rdd.map(_._2))
            println("原始数据数据存入" + Constants.INDEX_HDFS + "/" + typeName)
            EsSpark.saveToEs(hdfs, Constants.INDEX_HDFS + "/" + typeName)
          case Constants.TOPIC_HIVE =>val hive: RDD[HIVE] = Data2entry.transHIVE(rdd.map(_._2))
            println("原始数据数据存入" + Constants.INDEX_HIVE + "/" + typeName)
            EsSpark.saveToEs(hive.filter(_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + typeName)
            EsSpark.saveToEs(hive.filter(!_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + yesTypeName)

          case Constants.TOPIC_YARN =>val yarn: RDD[YARN] = Data2entry.transYARN(rdd.map(_._2))
            println("原始数据数据存入" + Constants.INDEX_YARN + "/" + typeName)
            EsSpark.saveToEs(yarn.filter(_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + typeName)
            EsSpark.saveToEs(yarn.filter(!_.opTime.split(" ")(0).equals(typeName)), Constants.INDEX_HDFS + "/" + yesTypeName)

        }

        println("原始数据数据存入ES完毕、、、、、、、、、、、将10秒批次数据写入redis短期存储")

        //更新偏移量到数据库
        UpdateOffsets.updateNewOff(offsetRanges,consumer)

      }

    })
  }

  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    dateFormat.format(cal.getTime())
  }

}
