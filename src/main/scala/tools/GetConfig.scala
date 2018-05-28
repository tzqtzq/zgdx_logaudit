package tools

import entry.HDFS
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by tianzhongqiu on 2018/5/24.
  */

object GetConfig {

  //只适用于类成员 不适用于局部变量
  @transient  private var instance: SparkConf = _

  def getsparkConfig (partitionSize:String) :SparkConf={

      if (instance == null) {
        instance = new SparkConf()
        .setMaster("yarn")
        .setAppName("底层组件审计日志实时监控平台")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.streaming.kafka.maxRatePerPartition","5000")
        .set("spark.sql.shuffle.partitions",partitionSize)
        .set("es.nodes", "10.142.106.84,10.142.106.85,10.142.106.86")//ES配置
        .set("es.port", "9200")
        .set(".es.nodes.wan.only", "true")
        .set("es.index.auto.create", "true")
          .registerKryoClasses(Array(classOf[HDFS]))
      }
      instance
    }

  }


