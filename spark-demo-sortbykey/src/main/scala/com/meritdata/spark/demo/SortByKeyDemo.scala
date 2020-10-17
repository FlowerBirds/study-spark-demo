package com.meritdata.spark.demo

import org.apache.spark.sql.SparkSession

object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SecondSort").master("local").getOrCreate()
    val lines = spark.sparkContext.textFile("D:\\sort.txt")
    val pairs = lines.map { line =>
      (
        new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line
      )
    }
    val sortedParis = pairs.sortByKey()
    val sortedLines = sortedParis.map(pairs => pairs._2)
    sortedLines.foreach(s => println(s))
    spark.stop()
  }
}

class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}