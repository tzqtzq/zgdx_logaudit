package realtime

import org.apache.spark.streaming.kafka.OffsetRange
import scalikejdbc.DB
import scalikejdbc.config.DBs
import scalikejdbc._
import scalikejdbc.config.DBs
/**
  * Created by tianzhongqiu on 2018/5/29.
  */
object UpdateOffsets {

  DBs.setup()
  def updateNewOff(offsetRanges: Array[OffsetRange],consumer:String): Unit ={

    offsetRanges.foreach(osr => {

      DB.localTx { implicit session =>
        sql"update hdfs_offset_test SET offsets=?  WHERE consumer=? AND topic=? AND partition_num=? ".bind( osr.untilOffset,consumer, osr.topic,osr.partition).update().apply()
      }

    })

    println("偏移量更新完毕....")
  }

  def insertNewOff(offsetRanges: Array[OffsetRange],consumer:String): Unit ={

    offsetRanges.foreach(osr => {

      DB.localTx { implicit session =>
        sql"update hdfs_offset_test SET offsets=?  WHERE consumer=? AND topic=? AND partition_num=? ".bind( osr.untilOffset,consumer, osr.topic,osr.partition).update().apply()
      }

    })

    println("新的分区偏移量插入完毕....")
  }

}
