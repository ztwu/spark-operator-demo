package com.iflytek.edcc.rdd

import org.apache.spark.Partitioner

class MySelfPartitioner(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val code = (key.asInstanceOf[Int] % numPartitions)
    println("===================")
    if(code < 0){
      code + numPartitions
    }else{
      code
    }
  }

  // 用来让Spark区分分区函数对象的Java equals方法
  override def equals(other: Any): Boolean = other match {
    case dnp: MySelfPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }
}