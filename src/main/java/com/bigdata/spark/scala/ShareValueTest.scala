package com.bigdata.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object ShareValueTest {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext
    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    val accum = sc.longAccumulator
    rdd.foreach(e => accum.add(e))
    println(accum)
  }

  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("ShareValueTest")
      .setMaster("local")
    new SparkContext(conf);
  }
}
