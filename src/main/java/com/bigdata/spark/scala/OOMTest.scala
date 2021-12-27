package com.bigdata.spark.scala
import org.apache.spark.{SparkConf, SparkContext}

object OOMTest {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext

    val dataRDD = sc.textFile("data\\video_info.log").cache()

    while (true){
      ;
    }

  }

  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local")
    new SparkContext(conf);
  }

}
