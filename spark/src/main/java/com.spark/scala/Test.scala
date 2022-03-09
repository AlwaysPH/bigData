package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext
    val data = Seq(1,2,3,4)
    method1(sc, data)
    method2(sc, data)
  }

  def method1(sc: SparkContext, data: Seq[Int]) = {
    var count = 0
    val rdd = sc.parallelize(data)
    // rdd的操作都被转换成 Job 分发至集群的执行器 Executor 进程中运行
    // foreach 中的匿名函数 e => count += e 首先会被序列化然后被传入计算节点，
    // 反序列化之后再运行，因为 foreach 是 Action 操作，结果会返回到 Driver 进程中
    rdd.foreach(e => count += e)
    println(count)
  }

  def method2(sc: SparkContext, data: Seq[Int]): Unit = {
    var count = 0
    data.foreach(e => count += e)
    println(count)
  }

  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local")
    new SparkContext(conf);
  }
}
