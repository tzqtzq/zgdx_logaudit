package batchlevel

import java.sql.{Timestamp, Connection}
import java.text.SimpleDateFormat
import java.util.Calendar
import jdbc.JDBCHelper
import org.apache.logging.log4j.scala.Logging
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
class ESOffline extends Logging{

  def main(args: Array[String]) {

    import ESOffline.sql.implicits._

     val hdfsDF: DataFrame = ESOffline.sql.esDF(Constants.INDEX_HDFS + ESOffline.getYesterday())
      .select("userName", "cmd", "ip", "src")
      .groupBy($"userName", $"cmd", $"ip", $"src").count()
      .toDF("userName", "cmd", "ip", "src", "count")


    hdfsDF.foreachPartition(it=>{

      val conn: Connection = JDBCHelper.getInstance().getConnection
      val sqlLine="insert into hdfs_data_test values(?,?,?,?,?,?)"
      it.foreach(row=>{

        val userName= row.getAs[String]("userName")
        val opTime=new Timestamp(System.currentTimeMillis()-86400000L)
        val cmd=row.getAs[String]("cmd")
        val ip=row.getAs[String]("ip")
        val src=row.getAs[String]("src")
        //                val dst=row.getAs[String]("dst")
        val count=row.getAs[Long]("count")
        try {

          val ps = conn.prepareStatement(sqlLine)
          ps.setString(1,userName)
          ps.setTimestamp(2,opTime)
          ps.setString(3,cmd)
          ps.setString(4,ip)
          ps.setString(5,src)
          //                  ps.setString(6,dst)
          ps.setLong(6,count)

          ps.executeUpdate()
          ps.close()

        }catch{
          case exception:Exception=>
            logger.error(s"exception.getMessage === ${exception.getMessage}")
            exception.printStackTrace()
        }
        conn.close()

      })
    })
    //  sql.esDF(Contains.INDEX_HDFS+getYesterday()).registerTempTable("tb_hive")
    //  sql.esDF(Contains.INDEX_HDFS+getYesterday()).registerTempTable("tb_yarn")



  }


}



object ESOffline{

  def apply()= new ESOffline()

  val sparkConf: SparkConf = GetConfig.getsparkConfig(Constants.SIZE_OFFLINE)
  val sc: SparkContext = new SparkContext(sparkConf)
  val sql: SQLContext = new SQLContext(sc)

  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    dateFormat.format(cal.getTime())
  }

}
