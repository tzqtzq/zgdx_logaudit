package tools

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.Jedis
import toolpool.SimpleDT

import scala.collection.mutable

/**
  * Created by tianzhongqiu on 2018/5/8.
  */


object Test extends Logging{

  def main(args: Array[String]) {

    val jedis: Jedis = JedisPools.getJedis()

//    jedis.hincrBy()

  }

}
