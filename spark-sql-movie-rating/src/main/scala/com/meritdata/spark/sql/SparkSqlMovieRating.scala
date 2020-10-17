package com.meritdata.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//照着字段自己给定义的类型,编辑样例类
case class User(userid: Int, sex: String, age: Int, occupation: String, zipcode: String)

case class Movie(movieid: Int, moviename: String, movietype: String)

case class Rating(userid: Int, movieId: Int, rate: Double, times: String)


object SparkSqlMovieRating {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSQL_Movie").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //rdd转DF需导入隐式转换
    import spark.implicits._

    //把数据转成数据集
    val user: Dataset[User] = spark.read.textFile("data/ml-1m/users.dat").map(_.split("::")).map(x => User(x(0).toInt, x(1), x(2).toInt, x(3), x(4)))
    val movie: Dataset[Movie] = spark.read.textFile("data/ml-1m/movies.dat").map(_.split("::")).map(x => Movie(x(0).toInt, x(1), x(2)))
    val rating: Dataset[Rating] = spark.read.textFile("data/ml-1m/ratings.dat").map(_.split("::")).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble, x(3)))
    //先查看相关数据
        user.show()
        movie.show()
        rating.show()

    //用sql实现
    //建表-临时表
    user.createOrReplaceTempView("t_user")
    movie.createOrReplaceTempView("t_movie")
    rating.createOrReplaceTempView("t_rating")

    //思路:复杂业务分解，把握核心
    //1、求被评分次数最多的 10 部电影，并给出评分次数（电影名，评分次数）
    val df1a: DataFrame = spark.sql(
      """
                select moviename,count(*) mcount from t_rating a join t_movie b on a.movieid=b.movieid group by moviename  order by mcount desc limit 10
              """)
    df1a.show()
  }

}
