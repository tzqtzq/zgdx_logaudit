package tools

import contains.Constants
import entry.{HIVE, YARN}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import reids.Jedisutil

/**
  * Created by tianzhongqiu on 2018/5/29.
  */
object Test1 {

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

  def main(args: Array[String]) {

    val config: SparkConf = GetConfig.getsparkConfig("8")
    //
        val context: SparkContext = new SparkContext(config)
        val file: RDD[String] = context.textFile("C:\\Users\\tianzhongqiu\\Desktop\\hive.txt")
    val d: RDD[(Int, String)] = context.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)

    import org.apache.hadoop.mapred.TextOutputFormat
    import org.apache.hadoop.io.Text
    import org.apache.hadoop.io.IntWritable
    import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
    file.foreach(line=>{
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
    case 8=>println(userName,opTime,logType,ip,split2(2).split("=")(1),"","","")
    case 9=>println(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(1)+" "+split(8),"","","")
    case 10=>if(split(8).contains("db")){println(userName,opTime,logType,ip,split2(2).split("=")(1),split(8).split("=")(1).trim,split(9).split("=")(1),"")}
    else {println(userName,opTime,logType,ip,split2(2).split("=")(1),"","","")}
    case 11=>println(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(1)+" "+split(8),split(9).split("=")(1).trim,"",split(10).split("=")(1).trim)
    case 12=>println(userName,opTime,logType,ip,split2(2).split(" ")(0).split("=")(0)+split(8)+split(9)+split(10)+split(11),"","","")
    case _=>println(line)
      println("", "", "", "", "", "","","")
  }
  } catch {
    case ex:ArrayIndexOutOfBoundsException => print(line+ex)
  }

})



//println(a)

//    js.expire()


  }

}
