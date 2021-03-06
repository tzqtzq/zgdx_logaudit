package batchlevel

import java.sql.{Timestamp, Connection}
import _root_.jdbc.JDBCHelper
import contains.Constants
import entry.{YARN, HIVE, HDFS}
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sql._
import org.slf4j.LoggerFactory
import tools.GetConfig

/**
  * Created by tianzhongqiu on 2018/6/8.
  * 测试
  */

object OfflineLogAudit {

  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(OfflineLogAudit.getClass)

    if (args.length != 3) {
      println("Usage:<inputPath><dataBaseName><userName><password>")
      sys.exit(1)
    }


    val config: SparkConf = GetConfig.getsparkConfig("15",args(1))

    val sc: SparkContext = new SparkContext(config)

    val scc: SQLContext = new SQLContext(sc)

    import  scc.implicits._

    val yarnDF: DataFrame = scc.esDF(Constants.INDEX_YARN + "/" + args(0))
      //        .filter(_.getAs[String]("opTime").split(" ")(0).equals(args(1)))
      .select("userName", "logType", "ip", "operation", "appID",  "result")
      .groupBy($"userName", $"logType", $"ip", $"operation", $"appID", $"result").count()
      .toDF("userName", "logType", "ip", "operation", "appID", "result", "count")


    yarnDF.foreachPartition(it => {

      val conn: Connection = JDBCHelper.getInstance().getConnection
      val sqlLine = "insert into yarn_data_test values(?,?,?,?,?,?,?,?)"
      it.foreach(row => {

        val userName = row.getAs[String]("userName")
        //        val opTime = new Timestamp(System.currentTimeMillis() - 86400000L)
        val opTime=java.sql.Date.valueOf(args(2))
        val logType = row.getAs[String]("logType")
        val ip = row.getAs[String]("ip")
        val operation = row.getAs[String]("operation")
        val appID = row.getAs[String]("appID")

        val result = row.getAs[String]("result")
        //                val dst=row.getAs[String]("dst")
        val count = row.getAs[Long]("count")
        try {

          val ps = conn.prepareStatement(sqlLine)
          ps.setString(1, userName)
          ps.setDate(2, opTime)
          ps.setString(3, logType)
          ps.setString(4, ip)
          ps.setString(5, operation)
          ps.setString(6, appID)

          ps.setString(7, result)
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


    logger.info("ES中index为 %s 的数据已经被处理完毕存入数据库中",Constants.INDEX_YARN)
  }

}
