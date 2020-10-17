package com.meritdata.spark.demo

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val spark = SparkSession.builder().appName("SortWordCount").master("local[2]").getOrCreate()
    // 读取文本文件，转化为RDD
    val lines = spark.sparkContext.textFile("data/voa_news1.txt")
    lines.count()
    // 用空格切分每行字符为单个单词
    val words = lines.flatMap{line => line.split(" ")}
    // 单词分组统计
    val wordCounts = words.map{word => (word,1)}.reduceByKey(_ + _)
    // 将统计结果展开为单词和次数
    val countWord = wordCounts.map{word =>(word._2,word._1)}
    // 使用单词排序
    val sortedCountWord = countWord.sortByKey(false)
    // 将排序结果展开
    val sortedWordCount = sortedCountWord.map{word => (word._2, word._1)}
    // 循环展示（action算子，触发job）
    sortedWordCount.foreach(s=>
    {
      println("word \""+s._1+ "\" appears "+s._2+" times.")
    })
    spark.stop()
  }

}
