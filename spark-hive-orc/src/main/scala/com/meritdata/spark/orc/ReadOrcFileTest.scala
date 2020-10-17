package com.meritdata.spark.orc

import org.apache.spark.sql.SparkSession

object ReadOrcFileTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.config("spark.driver.host", "localhost").appName("OrcFileTest").master("local").getOrCreate
    val df = spark.read.orc("F:\\workspace\\miniprogram\\study-spark-demo\\data\\statics_custom_crowd")
    df.show()
  }

}
