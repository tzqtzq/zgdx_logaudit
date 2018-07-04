package tools

import entry.{YARN, HIVE, HDFS}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

/**
  * Created by tianzhongqiu on 2018/5/24.
  */

object GetConfig {

  //只适用于类成员 不适用于局部变量
  @transient  private var instance: SparkConf = _

  def getsparkConfig (partitionSize:String,mode:String) :SparkConf={

      if (instance == null) {
        instance = new SparkConf()
        .setMaster(mode)
        .setAppName("底层组件审计日志实时监控平台")
        .set("spark.serializer", "org.apache.spark.ser ializer.KryoSerializer")
        .set("spark.streaming.kafka.maxRatePerPartition","90000")
        .set("spark.sql.shuffle.partitions",partitionSize)
        .set("es.nodes", "NM-402-HW-XH628V3-BIGDATA-2016-303.BIGDATA.CHINATELECOM.CN,NM-402-HW-XH628V3-BIGDATA-2016-302.BIGDATA.CHINATELECOM.CN,NM-402-HW-XH628V3-BIGDATA-2016-301.BIGDATA.CHINATELECOM.CN")//ES配置
        .set("es.port", "8200")
        .set("es.nodes.wan.only", "true")
        .set("es.index.auto.create", "true")
//            .set("es.mapping.date.rich", "false") //解决解析日期报异常的错误
//            .set("es.internal.spark.sql.pushdown.strict", "true")
//            .set("pushdown", "true")
//          注册监听器，监控task错误
//            .set("enableSendEmailOnTaskFail", "true")
//            .set("spark.extraListeners", "tools.I4SparkAppListener")
          .registerKryoClasses(Array(classOf[HDFS],classOf[HIVE],classOf[YARN]))
      }

      instance
    }
  }


