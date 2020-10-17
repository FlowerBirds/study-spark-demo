package com.meritdata.spark.orc

import org.apache.spark.sql.SparkSession

object ReadHiveTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      config("spark.driver.host", "localhost")
      .config("fs.defaultFS", "hdfs://localhost:9000")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
      .config("hive.input.dir.recursive", "true")
      .config("hive.mapred.supports.subdirectories", "true")
      .config("hive.supports.subdirectories", "true")
      .appName("OrcFileTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate


    //val df = spark.sql("select * from schoole")
    //df.show()

    val statics_custom_crowd = spark.sql("select * from statics_custom_crowd")
    statics_custom_crowd.explain()
    statics_custom_crowd.show()
    statics_custom_crowd.count()

    // spark.read.orc("hdfs://localhost:9000/user/hive/warehouse211/statics_custom_crowd").show()
  }

}
