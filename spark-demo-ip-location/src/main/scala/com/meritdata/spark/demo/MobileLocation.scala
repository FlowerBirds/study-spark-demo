package com.meritdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mobile_location案例
 */
object MobileLocation {

  def main(args: Array[String]) {
    //本地运行
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local")
    val sc = new SparkContext(conf)

    //todo:过滤出工作时间(读取基站用户信息:18688888888,20160327081200,CC0710CC94ECC657A8561DE549D940E0,1)
    val officetime = sc.textFile("e:\\ce\\*.log")
      .map(_.split(",")).filter(x => (x(1).substring(8, 14) >= "080000" && (x(1).substring(8, 14) <= "180000")))

    //todo:过滤出家庭时间(读取基站用户信息:18688888888,20160327081200,CC0710CC94ECC657A8561DE549D940E0,1)
    val hometime = sc.textFile("e:\\ce\\*.log")
      .map(_.split(",")).filter(x => (x(1).substring(8, 14) > "180000" && (x(1).substring(8, 14) <= "240000")))

    //todo:读取基站信息:9F36407EAD0629FC166F14DDE7970F68,116.304864,40.050645,6
    val rdd20 = sc.textFile("e:\\ce\\loc_info.txt")
      .map(_.split(",")).map(x => (x(0), (x(1), x(2))))

    //todo:计算多余的时间次数
    val map1Result = computeCount(officetime)
    val map2Result = computeCount(hometime)
    val mapBro1 = sc.broadcast(map1Result)
    val mapBro2 = sc.broadcast(map2Result)

    //todo:计算工作时间
    computeOfficeTime(officetime, rdd20, "c://out/officetime", mapBro1.value)

    //todo:计算家庭时间
    computeHomeTime(hometime, rdd20, "c://out/hometime", mapBro2.value)
    sc.stop()
  }

  /**
   * 计算多余的时间次数
   *
   * 1、将“电话_基站ID_年月日"按key进行分组，如果value的大小为2，那么证明在同一天同一时间段（8-18或者20-24）同时出现两次，那么这样的数据需要记录，减去多余的时间
   * 2、以“电话_基站ID”作为key，将共同出现的次数为2的累加，作为value，存到map中，
   * 例如：
   * 13888888888_8_20160808100923_1和13888888888_8_20160808170923_0表示在13888888888在同一天20160808的8-18点的时间段，在基站8出现入站和出站
   * 那么，这样的数据对于用户13888888888在8基站就出现了重复数据，需要针对key为13888888888_8的value加1
   * 因为我们计算的是几个月的数据，那么，其他天数也会出现这种情况，累加到13888888888_8这个key中
   */
  def computeCount(rdd1: RDD[Array[String]]): Map[String, Int] = {
    var map = Map(("init", 0))
    //todo:groupBy:按照"电话_基站ID_年月日"分组，将符合同一组的数据聚在一起
    for ((k, v) <- rdd1.groupBy(x => x(0) + "_" + x(2) + "_" + x(1).substring(0, 8)).collect()) {
      val tmp = map.getOrElse(k.substring(0, k.length() - 9), 0)
      if (v.size % 2 == 0) {
        //todo:以“电话_基站ID”作为key，将共同出现的次数作为value，存到map中
        map += (k.substring(0, k.length() - 9) -> (tmp + v.size / 2))
      }
    }
    map
  }

  /**
   * 计算在家的时间
   */
  def computeHomeTime(rdd1: RDD[Array[String]], rdd2: RDD[(String, (String, String))], outDir: String, map: Map[String, Int]) {

    //todo：（手机号_基站ID，时间）算法：24-x 或者 x-20
    val rdd3 = rdd1.map(x => ((x(0) + "_" + x(2), if (x(3).toInt == 1) 24 - Integer.parseInt(x(1).substring(8, 14)) / 10000
    else Integer.parseInt(x(1).substring(8, 14)) / 10000 - 20)))

    //todo：手机号_基站ID,总时间
    val rdd4 = rdd3.reduceByKey(_ + _).map {
      case (telPhone_zhanId, totalTime) => {
        (telPhone_zhanId, totalTime - (Math.abs(map.getOrElse(telPhone_zhanId, 0)) * 4))
      }
    }

    //todo：按照总时间排序(手机号_基站ID,总时间<倒叙>)
    val rdd5 = rdd4.sortBy(_._2, false)

    //todo：分割成：手机号,(基站ID,总时间)
    val rdd6 = rdd5.map {
      case (telphone_zhanId, totalTime) => (telphone_zhanId.split("_")(0), (telphone_zhanId.split("_")(1), totalTime))
    }

    //todo:找到时间的最大值:(手机号,compactBuffer((基站ID,总时间1),(基站ID,总时间2)))
    val rdd7 = rdd6.groupByKey.map {
      case (telphone, buffer) => (telphone, buffer.head)
    }.map {
      case (telphone, (zhanId, totalTime)) => (telphone, zhanId, totalTime)
    }

    //todo:join都获取基站的经纬度
    val rdd8 = rdd7.map {
      case (telphon, zhanId, time) => (zhanId, (telphon, time))
    }.join(rdd2).map {
      //todo:(a,(1,2))
      case (zhanId, ((telphon, time), (jingdu, weidu))) => (telphon, zhanId, jingdu, weidu)
    }
    rdd8.foreach(println)
    //rdd8.saveAsTextFile(outDir)
  }

  /**
   * 计算工作的时间
   */
  def computeOfficeTime(rdd1: RDD[Array[String]], rdd2: RDD[(String, (String, String))], outDir: String, map: Map[String, Int]) {

    //todo：（手机号_基站ID，时间） 算法：18-x 或者 x-8
    val rdd3 = rdd1.map(x => ((x(0) + "_" + x(2), if (x(3).toInt == 1) 18 - Integer.parseInt(x(1).substring(8, 14)) / 10000
    else Integer.parseInt(x(1).substring(8, 14)) / 10000 - 8)))

    //todo：手机号_基站ID,总时间
    val rdd4 = rdd3.reduceByKey(_ + _).map {
      case (telPhone_zhanId, totalTime) => {
        (telPhone_zhanId, totalTime - (Math.abs(map.getOrElse(telPhone_zhanId, 0)) * 10))
      }
    }

    //todo：按照总时间排序(手机号_基站ID,总时间<倒叙>)
    val rdd5 = rdd4.sortBy(_._2, false)

    //todo：分割成：手机号,(基站ID,总时间)
    val rdd6 = rdd5.map {
      case (telphone_zhanId, totalTime) => (telphone_zhanId.split("_")(0), (telphone_zhanId.split("_")(1), totalTime))
    }

    //todo:找到时间的最大值:(手机号,compactBuffer((基站ID,总时间1),(基站ID,总时间2)))
    val rdd7 = rdd6.groupByKey.map {
      case (telphone, buffer) => (telphone, buffer.head)
    }.map {
      case (telphone, (zhanId, totalTime)) => (telphone, zhanId, totalTime)
    }

    //todo:join都获取基站的经纬度
    val rdd8 = rdd7.map {
      case (telphon, zhanId, time) => (zhanId, (telphon, time))
    }.join(rdd2).map {
      case (zhanId, ((telphon, time), (jingdu, weidu))) => (telphon, zhanId, jingdu, weidu)
    }
    rdd8.foreach(println)
    //rdd8.saveAsTextFile(outDir)
  }
}
