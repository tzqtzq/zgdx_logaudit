package tools

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import contains.Constants
import entry.HDFS
import org.apache.spark.rdd.RDD
import toolpool.SimpleDT

/**
  * Created by tianzhongqiu on 2018/5/16.
  * 工具类
  */
object Data2entry {



  def getUserName(logType: String, line: String): String = {
    if (logType.equals(Constants.TYPE_HDFS)) {
      if (line.contains("@")) {
        if (line.contains("/")) {
          line.split("/")(0)
        } else {
          line.split("@")(0)
        }
      } else {
        line.trim
      }
    } else {
      "EEEEE:Type is not defined"
    }

  }

  def transHDFS(rdd: RDD[String]) = {

    rdd.map(line => {
      //  拆分数据计算指标数据
      val split: Array[String] = line.split(" ", -1)
      var opData: Date=null
      var splitTab: Array[String]=null
      var userName: String=""
      try{
        opData = SimpleDT.parse(split(0) + " " + split(1).split(",")(0)) //2018-05-04 15:40:18
        //分隔符更改为/t
        splitTab = (split(4) + split(5)).split("\t")

        userName= getUserName("hdfs", splitTab(1).split("=")(1)) //hjpt

      }catch {
        case ex:ArrayIndexOutOfBoundsException => print(line)
      }
           if (split.length == 6) {
        try {
          new HDFS(
            userName,
//            Str2TimeStamp.str2DateCom(split(0) + " " + split(1).split(",")(0)),
            split(0) + " " + split(1).split(",")(0),
            splitTab(0),
            splitTab(3).split("=")(1),
            splitTab(2).split("/")(1),
            splitTab(4).split("=")(1),
            splitTab(5).split("=")(1),
            splitTab(6).split("=")(1),
            splitTab(7).split("=")(1)
          )
        } catch {
          //          case ex: ArrayIndexOutOfBoundsException => print(line) // Handle missing file
          case ex:ArrayIndexOutOfBoundsException => print(line)
            new HDFS("", "", "", "", "", "", "", "", "")
        }
      } else {
                try{
                  val split1: Array[String] = line.split("\\s{1,}", -1)
                  new HDFS(
                    userName,
//                    Str2TimeStamp.str2DateCom(split(0) + " " + split(1).split(",")(0)),
                    split(0) + " " + split(1).split(",")(0),
                    splitTab(0),
                    split1(11).split("=")(1),
                    split1(10).split("/")(1),
                    split1(12).split("=")(1),
                    split1(13).split("=")(1),
                    split1(14).split("=")(1),
                    split1(15).split("=")(1)
                  )
                }catch {
                  case ex: ArrayIndexOutOfBoundsException => print(line)
                    new HDFS("", "", "", "", "", "", "", "", "")// Handle missing file
                }


      }
    })
  }
}



