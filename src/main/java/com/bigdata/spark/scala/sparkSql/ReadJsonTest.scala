package com.bigdata.spark.scala.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadJsonTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val sparkSession = SparkSession.builder()
      .appName("ReadJsonTest")
      .config(conf)
      .getOrCreate()

    val frame = sparkSession.read.json("data\\video_info.log")

    frame.createOrReplaceTempView("video_info")

    sparkSession.sql("select * from video_info").show()

    sparkSession.stop()

  }
}
