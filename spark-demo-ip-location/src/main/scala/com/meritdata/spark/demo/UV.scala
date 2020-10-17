package com.meritdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * UV（Unique Visitor）独立访客，统计1天内访问某站点的用户数(以cookie为依据);访问网站的一台电脑客户端为一个访客
 */
object UV {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("pv").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //读取数据access.log
    val file: RDD[String] = sc.textFile("e:\\access.log")

    //要分割file，拿到ip，然后去重
    val uvAndOne = file.map(_.split(" ")).map(x => x(0)).distinct().map(x => ("uv", 1))

    //聚合
    val result = uvAndOne.reduceByKey(_ + _)
    result.foreach(println)
  }
}
