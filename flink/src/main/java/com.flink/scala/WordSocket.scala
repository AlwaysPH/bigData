package com.flink.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 通过socket实时统计
 */
object WordSocket {

  def main(args: Array[String]): Unit = {
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    val text = evn.socketTextStream("bigData04", 9001)

    import org.apache.flink.api.scala._
    val wordCount = text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(s => s._1)
      .timeWindow(Time.seconds(2))
      .sum(1)

    wordCount.print().setParallelism(1)

    evn.execute("WordSocket")

  }

}
