package com.meritdata.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopK {

  def main(args: Array[String]) {
    //创建配置，设置app的name
    val conf = new SparkConf().setAppName("topk").setMaster("local[2]")
    //创建sparkcontext，将conf传进来
    val sc = new SparkContext(conf)
    //读取数据access.log
    val file: RDD[String] = sc.textFile("e:\\access.log")
    //将一行数据作为输入，将（）
    val refUrlAndOne: RDD[(String, Int)] = file.map(_.split(" ")).map(x => x(10)).map((_, 1))

    //聚合
    val result: Array[(String, Int)] = refUrlAndOne.reduceByKey(_ + _).sortBy(_._2, false).take(3)

    println(result.toList)
  }
}
