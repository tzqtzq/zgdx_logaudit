package tools

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import toolpool.sendmail.MainSender


/**
  * Created by tianzhongqiu on 2018/6/28.
  * task监控，出现失败的task发送邮件给管理员
  */
class I4SparkAppListener(conf: SparkConf) extends SparkListener with Logging {

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1) {
      val errorMessage: Option[String] =
        taskEnd.reason match {
          case kill: TaskKilled =>
            Some(kill.toErrorString)
          case e: ExceptionFailure =>
            Some(e.toErrorString)
          case e: TaskFailedReason =>
            Some(e.toErrorString)
          case _ => None
        }
      if (errorMessage.nonEmpty) {
        if (conf.getBoolean("enableSendEmailOnTaskFail", false)) {
          val args = Array("971314085@qq.com", "spark任务监控", errorMessage.get)
          try {
            new MainSender().sendEmail(errorMessage.get)
          } catch {
            case e: Exception =>
          }
        }
      }
    }
  }

}
