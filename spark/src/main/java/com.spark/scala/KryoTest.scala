package com.spark.scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KryoTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("KryoTest")
      .setMaster("local")
      //使用kryo序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //注册自定义数据类型
      .registerKryoClasses(Array(classOf[Person]))
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array("xiaohei xiaoming", "xiaohong xiaochou"))
    val mapRDD = dataRDD.flatMap(_.split(" "))
    val personRDD = mapRDD.map(e => Person(e,18)).persist(StorageLevel.MEMORY_ONLY_SER)
    personRDD.foreach(println(_))

    while(true){
      ;
    }
  }
}

case class Person(name:String, age:Int) extends Serializable
