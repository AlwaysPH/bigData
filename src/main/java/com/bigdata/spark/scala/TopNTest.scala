package com.bigdata.spark.scala

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object TopNTest {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext
    val videoData = sc.textFile("data\\video_info.log")
    val giftData = sc.textFile("data\\gift_record.log")

    //checkpoint
//    sc.setCheckpointDir("hdfs://bigData01:9000/check");
//    videoData.persist(StorageLevel.MEMORY_AND_DISK)
//    videoData.checkpoint()

    val videoRDD = videoData.map(e => {
      val json = JSON.parseObject(e)
      val uid = json.getString("uid")
      val vid = json.getString("vid")
      val area = json.getString("area")
      (vid, (uid, area))
    })

    val giftRDD = giftData.map(e => {
      val json = JSON.parseObject(e)
      val vid = json.getString("vid")
      val gold = Integer.parseInt(json.getString("gold"))
      (vid, gold)
    })

    val giftReduceRDD = giftRDD.reduceByKey(_ + _)

    val joinRDD = videoRDD.join(giftReduceRDD)

    val mapRDD = joinRDD.map(e => {
      val uid = e._2._1._1
      val area = e._2._1._2
      val gold = e._2._2
      ((uid, area), gold)
    })

    val reRDD = mapRDD.reduceByKey(_ + _)

    val value = reRDD.map(e => (e._1._2, (e._1._1, e._2))).groupByKey()

    val result = value.map(e => {
      val list = e._2.toList
      (e._1, list.sortBy(_._2).reverse.take(3).map(t => t._1 + ":" + t._2).mkString(","))
    })

    result.foreach(e => println(e))
    sc.stop()
  }

  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local")
    new SparkContext(conf);
  }

}
