package com.spark.scala.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RDDToDataFrameTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    val sparkSession = SparkSession.builder()
      .appName("RDDToDataFrameTest")
      .config(conf)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val dataRDD = sc.parallelize(Array(("xiaoming", 18), ("xiaohei", 20), ("xiaoqiang", 22)))

    //导入隐式转换
    import sparkSession.implicits._
    //RDD转换为dataFrame
    val dataFrame = dataRDD.map(e => Student(e._1, e._2)).toDF()
    dataFrame.show()

    //dataFrame转换为RDD
    val rdd = dataFrame.rdd

  }

}

case class Student(name:String, age:Int) extends Serializable
