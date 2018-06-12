package tools

import entry.YARN
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tianzhongqiu on 2018/5/30.
  */
object Test3 {

  def main(args: Array[String]) {

    val a="13331186345|20180414124040|0000|mbl_dpi-208235270_20180416001034587_2.gz|||||S0beijing001606|未识别业务||||||{\"url\":{\"age\":\"4.2.1\",\"urlBusi\":\"0\",\"urlRuleID\":\"155662\"}}|811|460031324543930|13331186345||ctnet@mycdma.cn|103.18.209.39|80|10.54.3.27|49291|115.168.64.232|172.16.99.101|115.168.32.140|0|3600000133E3||33|15|F99|20180414124040|20180414124107|27262|940|686|10|7|20180414124040|0|"


    val b="18917261158|20180414124105|0000|mbl_dpi-208235270_20180416001034587_2.gz|A009000307|百度地图|||S000403830000|百度||||||{\"url\":{\"siteRuleID\":\"41383\",\"age\":\"6\",\"urlBusi\":\"1\",\"urlRuleID\":\"1003\"}}|831|460036181130612|18917261158||ctwap@mycdma.cn|180.163.198.48|80|10.111.176.82|52270|115.168.64.248|172.16.97.161|115.168.73.163|0|360000037A43||59|1|399|20180414124105|20180414124107|1745|8906|1582|11|13|20180414124105|0||%E7%99%BE%E5%BA%A6%E5%9C%B0%E5%9B%BE/10.6.5.2 CFNetwork/711.3.|http://boscdn.baidu.com/map-mobile-opnimg/632ee76d9c754605ba96fc026ed3ceec.png|boscdn.baidu.com|boscdn.baidu.com|3480|image/png|1|https://www.baidu.com/|6|200|108|0"
    val split: Array[String] = b.split("\\|",-1)

    print(split.length)

  }

}
