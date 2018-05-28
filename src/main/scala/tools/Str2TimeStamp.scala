package tools

import java.sql.Timestamp
import java.text.SimpleDateFormat

object Str2TimeStamp {

  val TIME_FORMAT_CONMENT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TIME_FORMAT_D = new SimpleDateFormat("yyyy-MM-dd")
  val DATE_FORMAT_D = new SimpleDateFormat("yyyyMMdd")
  val TIME_FORMAT_OPENAPI = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")


  //普通格式的时间转换
  def str2DateCom(dateStr:String):Timestamp= {

    dateStr match {
      case "" => Timestamp.valueOf("1999-09-09 09:09:09")
      case null  => Timestamp.valueOf("1999-09-09 09:09:09")
      case _ => Timestamp.valueOf(dateStr)
    }
  }

  //OpenAPI的时间格式转换
//  def str2DateOpen(dateStr:String):Timestamp={
//
//    dateStr match {
//      case "" => Timestamp.valueOf("1999-09-09 09:09:09")
//      case null  => Timestamp.valueOf("1999-09-09 09:09:09")
//      case _ => Timestamp.valueOf(SimpleDT.formatDate(SimpleDT.parse1(dateStr)))
//    }
//
//  }

}
