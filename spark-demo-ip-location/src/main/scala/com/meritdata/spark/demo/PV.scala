package com.meritdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * PV（Page View）访问量, 即页面浏览量或点击量
 */
object PV {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("pv").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //读取数据access.log
    val file: RDD[String] = sc.textFile("e:\\access.log")
    //将一行数据作为输入，将（）
    val pvAndOne: RDD[(String, Int)] = file.map(x => ("pv", 1))
    //聚合计算
    val result = pvAndOne.reduceByKey(_ + _)
    result.foreach(println)
  }
}
