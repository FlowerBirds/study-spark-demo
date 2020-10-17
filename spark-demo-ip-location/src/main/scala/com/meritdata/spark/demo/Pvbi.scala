package com.meritdata.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * pv uv环比分析
 */
object Pvbi {

  val conf = new SparkConf().setAppName("pv").setMaster("local[7]")
  val sc = new SparkContext(conf)
  val PVArr = ArrayBuffer[(String, Int)]()
  val UVArr = ArrayBuffer[(String, Int)]()

  def main(args: Array[String]) {
    computePVOneDay("e:\\access/tts7access20140824.log")
    computePVOneDay("e:\\access/tts7access20140825.log")
    computePVOneDay("e:\\access/tts7access20140826.log")
    computePVOneDay("e:\\access/tts7access20140827.log")
    computePVOneDay("e:\\access/tts7access20140828.log")
    computePVOneDay("e:\\access/tts7access20140829.log")
    computePVOneDay("e:\\access/tts7access20140830.log")
    println(PVArr)
    computeUVOneDay("e:\\access/tts7access20140824.log")
    computeUVOneDay("e:\\access/tts7access20140825.log")
    computeUVOneDay("e:\\access/tts7access20140826.log")
    computeUVOneDay("e:\\access/tts7access20140827.log")
    computeUVOneDay("e:\\access/tts7access20140828.log")
    computeUVOneDay("e:\\access/tts7access20140829.log")
    computeUVOneDay("e:\\access/tts7access20140830.log")
    println(UVArr)
  }

  def computePVOneDay(filePath: String): Unit = {
    val file = sc.textFile(filePath)
    val pvTupleOne = file.map(x => ("pv", 1)).reduceByKey(_ + _)
    val collect: Array[(String, Int)] = pvTupleOne.collect()
    PVArr.+=(collect(0))
  }

  def computeUVOneDay(filePath: String): Unit = {
    val rdd1 = sc.textFile(filePath)
    val rdd3 = rdd1.map(x => x.split(" ")(0)).distinct
    val rdd4 = rdd3.map(x => ("uv", 1))
    val rdd5 = rdd4.reduceByKey(_ + _)
    val collect: Array[(String, Int)] = rdd5.collect()
    UVArr.+=(collect(0))
  }
}
