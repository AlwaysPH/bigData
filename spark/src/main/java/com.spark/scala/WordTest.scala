package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object WordTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local")
    val sc = new SparkContext(conf);

    val array = Array(1,2,3,4,5,6,7,8,9)

    val rdd = sc.parallelize(array)

    val sum = rdd.reduce(_ + _)

    println(sum)
  }

}
