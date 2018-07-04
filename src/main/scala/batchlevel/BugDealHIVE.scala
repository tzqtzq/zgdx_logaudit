package batchlevel

import java.sql.Connection

import _root_.jdbc.JDBCHelper
import contains.Constants
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import tools.GetConfig
import org.elasticsearch.spark.sql._
/**
  * Created by tianzhongqiu on 2018/6/20.
  */
object BugDealHIVE {


  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(OfflineLogAudit.getClass)

    val config: SparkConf = GetConfig.getsparkConfig("15",args(0))

    val sc: SparkContext = new SparkContext(config)

    val scc: SQLContext = new SQLContext(sc)

    import  scc.implicits._

    val bugDF: DataFrame = scc.esDF(Constants.INDEX_HIVE + "/" + "bugdeal")
      .filter(_.getAs[String]("opTime").split(" ")(0).equals(args(1)))
      .select("userName", "logType", "ip", "cmd", "db", "tabl")
      .groupBy($"userName", $"logType", $"cmd", $"ip", $"db", $"tabl").count()
      .toDF("userName", "logType", "ip", "cmd", "db", "tabl", "count")

    bugDF.foreachPartition(it => {

      val conn: Connection = JDBCHelper.getInstance().getConnection
      val sqlLine = "insert into hive_data_test values(?,?,?,?,?,?,?,?)"

      it.foreach(row => {

        val userName = row.getAs[String]("userName")
        //        val opTime = new Timestamp(System.currentTimeMillis() - 86400000L)
        val opTime = args(0)
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
          ps.setString(2, opTime)
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
