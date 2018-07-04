package tools

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import contains.Constants
import entry.{HIVE, YARN, HDFS}
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
        line.split(" ")(0)
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

  def transYARN(rdd: RDD[String]) = {

    rdd.filter(_.split("\t").length!=7).filter(_.split("\t").length!=6).map(line => {
      //  拆分数据计算指标数据
      var split: Array[String] =null
      var userName = ""
      var logType=""
      var opTime = ""
      var operation = ""
      var split1: Array[String]=null
      var targetName=""
      var result=""

      try {
         split = line.split(" ", -1)
         val split2: Array[String] = line.split("\t",-1)
        //正常数据jialeip
         val nomalData: Array[String] = line.split("\\s{1,}", -1)
         userName = split(1).split("\\|")(6).split("=")(1).split("\t")(0)
         logType=split(1).split("\\|")(2)
         opTime = split(0) + " " + split(1).split("\\|")(0)
//         operation = split(1).split("=")(2) + " " + split(2) + " " + split(3).split("\t")(0)
         split1 = split(3).split("\t")
         split1.length match {
           case 5 =>new YARN(userName,logType,opTime,split(1).split("=")(2) + " " + split(2) + " " + split(3).split("\t")(0),"",split1(1).split("=")(1),split1(2).split("=")(1),"","",split1(3).split("=")(1),split1(4).split("=")(1))
           case 4 =>if(split1(3).split("CONTAINERID").length!=1)
           {new YARN(userName,logType,opTime,split(1).split("=")(2) + " " + split(2) + " " + split(3).split("\t")(0),"",split1(1).split("=")(1),split1(2).split("=")(1),"","",split1(3).split("CONTAINERID")(0).split("=")(1),split1(3).split("CONTAINERID")(1).split("=")(1))}
           else {new YARN(userName,logType,opTime,nomalData(3).split("=")(1)+nomalData(4)+nomalData(5),nomalData(2).split("=")(1),nomalData(6).split("=")(1),nomalData(7).split("=")(1),nomalData(8).split("=")(1)+nomalData(9)+nomalData(10)+nomalData(11)+nomalData(12)+nomalData(13)+nomalData(14)+nomalData(15)+nomalData(16)+nomalData(17)+nomalData(18)+nomalData(19),nomalData(20).split("=")(1)+nomalData(21)+nomalData(22)+nomalData(23)+nomalData(24),nomalData(25).split("=")(1),nomalData(26).split("=")(1))}
           case 2=>new YARN(userName,logType,opTime,nomalData(3).split("=")(1)+nomalData(4)+nomalData(5),nomalData(2).split("=")(1),nomalData(6).split("=")(1),nomalData(7).split("=")(1),nomalData(8).split("=")(1)+nomalData(9)+nomalData(10)+nomalData(11)+nomalData(12)+nomalData(13)+nomalData(14)+nomalData(15)+nomalData(16)+nomalData(17)+nomalData(18)+nomalData(19),nomalData(20).split("=")(1)+nomalData(21)+nomalData(22)+nomalData(23)+nomalData(24),nomalData(25).split("=")(1),nomalData(26).split("=")(1))
           case 1=> if(split2.length==5){new YARN(userName,logType,opTime,split(1).split("=")(2) + " "+split(2)+split(3)+split(4).split("\t")(0),"",split2(2).split("=")(1),split2(3).split("=")(1),"","",split2(4).split("=")(1),"")}
           else {new YARN(userName,logType,opTime,split(1).split("=")(2) + " "+split(2)+split(3)+split(4).split("\t")(0),"",split2(2).split(" ")(0).split("=")(1),split2(2).split(" ")(3).split("=")(1),"","",split2(3).split("=")(1),"")}
           case _ => print(line)
                     new YARN("", "", "", "", "", "", "","", "", "","")
         }
      } catch {
        case ex:ArrayIndexOutOfBoundsException => print(line+ex)
          new YARN("", "", "", "", "", "", "","", "", "", "")
      }
    })
  }

  def transHIVE(rdd: RDD[String]) = {

    rdd.map(line => {
      //  拆分数据计算指标数据
      var split: Array[String] =null
      var userName = ""
      var opTime = ""
      var ip = ""
      var split2: Array[String] =null
      var logType=""
        try {
        split = line.split(" ", -1)
        split2= split(7).split("\t")
        userName = getUserName("hdfs",split2(0).split("=")(1))
        opTime = split(0) + " " + split(1).split(",")(0)
        ip = split2(1).split("/")(1)
        logType=split(2)
        split.length match {
          case 8=>new HIVE(userName,opTime,logType,ip,split2(2).split("=")(1),"","","")
          case 9=>new HIVE(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(1)+" "+split(8),"","","")
          case 10=>if(split(8).contains("db")){new HIVE(userName,opTime,logType,ip,split2(2).split("=")(1),split(8).split("=")(1).trim,split(9).split("=")(1),"")}
          else {new HIVE(userName,opTime,logType,ip,split2(2).split("=")(1),"","","")}
          case 11=>new HIVE(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(1)+" "+split(8),split(9).split("=")(1).trim,"",split(10).split("=")(1).trim)
          case 12=>new HIVE(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(0)+split(8)+split(9)+split(10)+split(11),"","","")
          case _=>
            new HIVE(userName,opTime,logType,ip,split2(2).split("=")(1),line.split("[(]")(1).split(",")(0).split(":")(1),"",line.split("[(]")(1).split(":")(1).split(",")(0))
        }
        } catch {
          case ex:ArrayIndexOutOfBoundsException => print(line+ex)
            new HIVE("", "", "", "", "", "","","")
        }
    })
  }
}



