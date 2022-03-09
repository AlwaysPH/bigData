package com.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object TransformationTest {

  def main(args: Array[String]): Unit = {
    val sc =  getSparkContext
    //map
//    mapOp(sc)

    //filter
//    filterOp(sc)

    //groupByKey
//    groupByKey(sc)

    //reduceByKey
//    reduceByKeyOp(sc)
  }

  def mapOp(sc: SparkContext): Unit = {
    val mapRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    mapRDD.map(_ * 2).foreach(e => println(e))
  }

  def filterOp(sc: SparkContext): Unit = {
    val filterRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))
    filterRDD.filter(_ % 2 != 0).foreach(println(_))
  }

  /**
   * 相同的key聚合
   * @param sc
   */
  def groupByKey(sc: SparkContext): Unit = {
    val list = List(("力量", "撼地神牛"), ("敏捷", "影魔"), ("敏捷", "幻影刺客"), ("智力", "卡尔"), ("智力", "冰女妹妹"))
    val listRDD = sc.parallelize(list)
    val keyRDD = listRDD.groupByKey()
    keyRDD.foreach(e => {
      val key = e._1
      val iterable = e._2.iterator
      var pl = ""
      while (iterable.hasNext){
        pl = pl + iterable.next() + ","
      }
      var result = ""
      if(pl.length > 0){
        result = pl.dropRight(1)
      }
      println("类型:" + key + " 模型:" + result)
    })
  }

  /**
   * 将RDD中所有K,V对中K值相同的V进行合并
   * @param sc
   */
  def reduceByKeyOp(sc: SparkContext): Unit = {
    val list = List(("力量", 12), ("敏捷", 20), ("敏捷", 10), ("智力", 10), ("智力", 10))
    val listRDD = sc.parallelize(list)
    val reduceRDD = listRDD.reduceByKey(_ + _).sortBy(_._2)
    reduceRDD.foreach(e => println("类型:" + e._1 + " 人数:" + e._2))
  }

  private def getSparkContext = {
    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local")
    new SparkContext(conf);
  }
}
