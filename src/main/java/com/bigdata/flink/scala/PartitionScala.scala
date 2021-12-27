package com.bigdata.flink.scala

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object PartitionScala {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //默认情况下flink任务中算子的并行度为当前CPU的个数
    //fromCollection的并行度为1
    val text = env.fromCollection(Array(1,2,3,4,5,6,7,8,9,10))

    //shuffle
//    shuffleOp(text)

    //rebalance
//    rebalance(text)

    //自定义分区
    customerPartitionOp(text)
    env.execute("PatitionScala")
  }

  private def customerPartitionOp(text: DataStream[Int]) = {
    text.map(e => e).setParallelism(2).partitionCustom(new MyPartitioner, e => e).print().setParallelism(4)
  }

  private def rebalance(text: DataStream[Int]) = {
    text.map(e => e).setParallelism(2).rebalance.print().setParallelism(4)
  }

  private def shuffleOp(text: DataStream[Int]) = {
    text.map(e => e).setParallelism(2).shuffle.print().setParallelism(4)
  }


}
