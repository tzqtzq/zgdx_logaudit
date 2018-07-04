package tools

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by tianzhongqiu on 2018/6/11.
  */
object Test4 {

  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    dateFormat.format(cal.getTime())
  }

  def main(args: Array[String]) {

    val typeName: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
    println(getYesterday)

  }

}
