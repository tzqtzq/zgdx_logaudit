package tools

import entry.YARN
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
  * Created by tianzhongqiu on 2018/5/30.
  */
object Test3 {


  def main(args: Array[String]) {

    val logger = LoggerFactory.getLogger(Test3.getClass)

    val a="fesf"

    logger.info("fsaef %s",a)
    logger.warn("fsaef %s",a)

  }

}
