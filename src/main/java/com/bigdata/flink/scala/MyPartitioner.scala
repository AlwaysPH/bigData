package com.bigdata.flink.scala

import org.apache.flink.api.common.functions.Partitioner

class MyPartitioner extends Partitioner[Int] {
  override def partition(k: Int, i: Int): Int = {
    if(k % 2 == 0){
      0
    }else{
      1
    }
  }
}
