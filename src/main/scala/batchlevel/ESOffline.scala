package batchlevel

import java.sql.{Timestamp, Connection}
import java.text.SimpleDateFormat
import java.util.Calendar
import entry.{YARN, HIVE, HDFS}
import jdbc.JDBCHelper
import org.slf4j.LoggerFactory
import scalikejdbc._
import scalikejdbc.config.DBs
import contains.Constants
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession.Builder
import tools.GetConfig
import org.elasticsearch.spark.sql._

/**
  * Created by tianzhongqiu on 2018/5/24.
  * 定时聚合昨天的聚合结果 批次跑任务 存到数据库中 提供查询
  */
//class ESOffline {
//  def getYesterday():String= {
//    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyMMdd")
//    var cal: Calendar = Calendar.getInstance()
//    cal.add(Calendar.DATE, -1)
//    dateFormat.format(cal.getTime())
//  }
//
//
//}

object ESOffline{

//  def apply()= new ESOffline()


  def main(args: Array[String]) {
    val logger = LoggerFactory.getLogger(ESOffline.getClass)


      //校检下参数的长度是否符合标准
        if (args.length != 2) {
          println("Usage:<inputPath><dataBaseName><userName><password>")
          sys.exit(1)
        }


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
      import scc.implicits._

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

