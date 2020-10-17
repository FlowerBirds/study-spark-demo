package com.meritdata.spark.demo

import org.apache.spark.sql.SparkSession

object GroupTop3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GroupTop3").master("local").getOrCreate()
    //创建初始RDD
    val lines = spark.sparkContext.textFile("score.txt")
    //对初始RDD的文本行按空格分割，映射为key-value键值对
    val pairs = lines.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))
    //对pairs键值对按键分组
    val groupedPairs = pairs.groupByKey()
    //获取分组后每组前3的成绩
    val top3Score = groupedPairs.map(classScores => {
      var className = classScores._1
      //获取每组的成绩，将其转换成一个数组缓冲,并按从大到小排序，取其前三
      var top3 = classScores._2.toBuffer.sortWith(_>_).take(3)
      Tuple2(className,top3)
    })
    top3Score.foreach(m => {
      println(m._1)
      for(s <- m._2) println(s)
      println("------------------")
    })
  }
}
