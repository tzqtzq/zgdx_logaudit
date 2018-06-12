package batchlevel

import java.sql.{Timestamp, Connection}
import _root_.jdbc.JDBCHelper
import contains.Constants
import entry.{YARN, HIVE, HDFS}
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sql._
import org.slf4j.LoggerFactory

/**
  * Created by tianzhongqiu on 2018/6/8.
  * 测试
  */

object OfflineLogAudit {

  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(OfflineLogAudit.getClass)

    val conf=new SparkConf()
      .setMaster("local[*]")
      .setAppName("底层组件审计日志实时监控平台")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions","15")
      .set("es.nodes", "NM-402-HW-XH628V3-BIGDATA-2016-303.BIGDATA.CHINATELECOM.CN,NM-402-HW-XH628V3-BIGDATA-2016-302.BIGDATA.CHINATELECOM.CN,NM-402-HW-XH628V3-BIGDATA-2016-301.BIGDATA.CHINATELECOM.CN")//ES配置
      .set("es.port", "8200")
      .set("es.nodes.wan.only", "true")
      .set("es.index.auto.create", "true")
      //            .set("es.internal.spark.sql.pushdown.strict", "true")
      //            .set("pushdown", "true")
      .registerKryoClasses(Array(classOf[HDFS],classOf[HIVE],classOf[YARN]))

    val sc: SparkContext = new SparkContext(conf)

    val scc: SQLContext = new SQLContext(sc)

    import  scc.implicits._

    val hiveDF: DataFrame = scc.esDF(Constants.INDEX_HIVE + "/" + args(0))
      .filter(_.getAs[String]("opTime").split(" ")(0).equals(args(1)))
      .select("userName", "logType", "ip", "cmd", "db", "tabl")
      .groupBy($"userName", $"logType", $"cmd", $"ip", $"db", $"tabl").count()
      .toDF("userName", "logType", "ip", "cmd", "db", "tabl", "count")

    hiveDF.foreachPartition(it => {

      val conn: Connection = JDBCHelper.getInstance().getConnection
      val sqlLine = "insert into hive_data_test values(?,?,?,?,?,?,?,?)"

      it.foreach(row => {

        val userName = row.getAs[String]("userName")
        val opTime = new Timestamp(System.currentTimeMillis() - 86400000L)
        val logType = row.getAs[String]("logType")
        val ip = row.getAs[String]("ip")
        val cmd = row.getAs[String]("cmd")
        val db = row.getAs[String]("db")
        val tabl = row.getAs[String]("tabl")
        //                val dst=row.getAs[String]("dst")
        val count = row.getAs[Long]("count")
        try {

          val ps = conn.prepareStatement(sqlLine)
          ps.setString(1, userName)
          ps.setTimestamp(2, opTime)
          ps.setString(3, logType)
          ps.setString(4, cmd)
          ps.setString(5, ip)
          ps.setString(6, db)
          ps.setString(7, tabl)
          //                  ps.setString(6,dst)
          ps.setLong(8, count)

          ps.executeUpdate()
          ps.close()

        } catch {
          case exception: Exception =>
            logger.error(exception.getMessage, exception)
            exception.printStackTrace()
        }

      })
      conn.close()
      logger.info("Yarn的关闭数据库连接")
    })
    logger.info("ES中index为 %d 的数据已经被处理完毕存入数据库中"+Constants.INDEX_HIVE)

  }

}
