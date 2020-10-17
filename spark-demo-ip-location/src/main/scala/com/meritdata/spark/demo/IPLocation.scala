package com.meritdata.spark.demo

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {

  def ip2Long(ip: String): Long = {
    //ip转数字口诀
    //分金定穴循八卦，toolong插棍左八圈
    val split: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- split) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //二分法查找
  def binarySearch(ipNum: Long, value: Array[(String, String, String, String, String)]): Int = {
    //上下循环循上下，左移右移寻中间
    var start = 0
    var end = value.length - 1
    while (start <= end) {
      val middle = (start + end) / 2
      if (ipNum >= value(middle)._1.toLong && ipNum <= value(middle)._2.toLong) {
        return middle
      }
      if (ipNum > value(middle)._2.toLong) {
        start = middle
      }
      if (ipNum < value(middle)._1.toLong) {
        end = middle
      }
    }
    -1
  }

  def data2MySQL(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_count (location, total_count) VALUES (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.74.100:3306/test", "root", "123456")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("iplocation").setMaster("local[5]")
    val sc = new SparkContext(conf)
    //读取数据(ipstart,ipend,城市基站名，经度，维度)
    val jizhanRDD = sc.textFile("E:\\ip.txt").map(_.split("\\|")).map(x => (x(2), x(3), x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8) + "-" + x(9), x(13), x(14)))
    //    jizhanRDD.foreach(println)
    //把RDD转换成数据
    val jizhanPartRDDToArray: Array[(String, String, String, String, String)] = jizhanRDD.collect()
    //广播变量，一个只读的数据区，是所有的task都能读取的地方，相当于mr的分布式内存
    val jizhanRDDToArray: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanPartRDDToArray)
    //    println(jizhanRDDToArray.value)

    val IPS = sc.textFile("E:\\20090121000132.394251.http.format").map(_.split("\\|")).map(x => x(1))

    //把ip地址转换为Long类型，然后通过二分法去ip段数据中查找，对找到的经纬度做wordcount
    //((经度，纬度),1)
    val result = IPS.mapPartitions(it => {

      val value: Array[(String, String, String, String, String)] = jizhanRDDToArray.value
      it.map(ip => {
        //将ip转换成数字
        val ipNum: Long = ip2Long(ip)

        //拿这个数字去ip段中通过二分法查找，返回ip在ip的Array中的角标
        val index: Int = binarySearch(ipNum, value)
        //通Array拿出想要的数据
        ((value(index)._4, value(index)._5), 1)
      })
    })

    //聚合操作
    val resultFinnal: RDD[((String, String), Int)] = result.reduceByKey(_ + _)

    //    resultFinnal.foreach(println)
    //将数据存储到数据库
    resultFinnal.map(x => (x._1._1 + "-" + x._1._2, x._2)).foreachPartition(data2MySQL _)
    sc.stop()

  }
}
