package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object FileRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("FileRDD")
      .setMaster("local")
    val sc = new SparkContext(conf);

    val fileRDD = sc.textFile("data\\words.txt", 2)

    val res = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey((x, y) => x + y).sortBy(_._2)

    res.foreach( e => println(e._1 + "~" + e._2))
  }
}
