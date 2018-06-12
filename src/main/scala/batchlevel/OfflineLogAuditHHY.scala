package batchlevel

import java.sql.{Timestamp, Connection}
import java.text.SimpleDateFormat
import java.util.Calendar

import _root_.jdbc.JDBCHelper
import contains.Constants
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.slf4j.LoggerFactory
import tools.GetConfig
import org.elasticsearch.spark.sql._

/**
  * Created by tianzhongqiu on 2018/6/8.
  * 测试
  */
object OfflineLogAuditHHY {

  def getYesterday():String= {
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyMMdd")
        var cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        dateFormat.format(cal.getTime())
      }

  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(OfflineLogAuditHHY.getClass)

    //校检下参数的长度是否符合标准
      if (args.length != 2) {
        println("Usage:<inputPath><dataBaseName><userName><password>")
        sys.exit(1)
      }


      val config: SparkConf = GetConfig.getsparkConfig("15")
      val scc: SQLContext = SQLContextSingleton.getInstance(new SparkContext(config))

      import scc.implicits._

      val hdfsDF: DataFrame = scc.esDF(Constants.INDEX_HDFS + "/" + args(0))
//        .filter(_.getAs[String]("opTime"))
        .select("userName", "cmd", "ip", "src")
        .groupBy($"userName", $"cmd", $"ip", $"src").count()
        .toDF("userName", "cmd", "ip", "src", "count")

    val hiveDF: DataFrame = scc.esDF(Constants.INDEX_HIVE + "/" + args(0))
        .select("userName", "logType", "ip", "cmd", "db", "tabl")
          .groupBy($"userName", $"logType", $"cmd", $"ip", $"db", $"tabl").count()
      .toDF("userName", "logType", "ip", "cmd", "db", "tabl", "count")

      val yarnDF: DataFrame = scc.esDF(Constants.INDEX_YARN + "/" + args(0))
  //        .filter(_.getAs[String]("opTime").split(" ")(0).equals(args(1)))
       .select("userName", "logType", "ip", "operation", "appID",  "result")
        .groupBy($"userName", $"logType", $"ip", $"operation", $"appID", $"result").count()
          .toDF("userName", "logType", "ip", "operation", "appID", "result", "count")

      hdfsDF.foreachPartition(it => {

        val conn: Connection = JDBCHelper.getInstance().getConnection
        val sqlLine = "insert into hdfs_data_test values(?,?,?,?,?,?)"
        it.foreach(row => {

          val userName = row.getAs[String]("userName")
          val opTime = new Timestamp(System.currentTimeMillis() - 86400000L)
          val cmd = row.getAs[String]("cmd")
          val ip = row.getAs[String]("ip")
          val src = row.getAs[String]("src")
          //                val dst=row.getAs[String]("dst")
          val count = row.getAs[Long]("count")
          try {

            val ps = conn.prepareStatement(sqlLine)
            ps.setString(1, userName)
            ps.setTimestamp(2, opTime)
            ps.setString(3, cmd)
            ps.setString(4, ip)
            ps.setString(5, src)
            //                  ps.setString(6,dst)
            ps.setLong(6, count)

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
      logger.info("ES中index为 %s 的数据已经被处理完毕存入数据库中",Constants.INDEX_HDFS)


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


    yarnDF.foreachPartition(it => {

      val conn: Connection = JDBCHelper.getInstance().getConnection
      val sqlLine = "insert into yarn_data_test values(?,?,?,?,?,?,?,?)"
      it.foreach(row => {

        val userName = row.getAs[String]("userName")
        val opTime = new Timestamp(System.currentTimeMillis() - 86400000L)
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
          ps.setTimestamp(2, opTime)
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

object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
