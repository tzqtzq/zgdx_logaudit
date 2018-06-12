package tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory


/**
  * Created by tianzhongqiu on 2018/6/1.
  */
object StopSCCWithElegant {

  val logger = LoggerFactory.getLogger(StopSCCWithElegant.getClass)

  /***
    * 通过一个消息文件来定时触发是否需要关闭流程序
    *
    * @param ssc StreamingContext
    */
  def stopByMarkFile(ssc:StreamingContext):Unit= {
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfs_file_path = "hdfs://NM-304-SA5212M4-BIGDATA-776:54310/user/tianzhongqiu/spark/streaming/stop" //判断消息文件是否存在，如果存在就
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hdfs_file_path)) {
        logger.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }

    }
  }

  /***
    * 判断是否存在mark file
    *
    * @param hdfs_file_path  mark文件的路径
    * @return
    */
  def isExistsMarkFile(hdfs_file_path:String):Boolean={
    val conf = new Configuration()
    val path=new Path(hdfs_file_path)
    val fs =path.getFileSystem(conf);
    fs.exists(path)
  }

}
