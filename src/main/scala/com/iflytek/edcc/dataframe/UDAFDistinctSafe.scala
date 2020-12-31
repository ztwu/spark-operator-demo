package com.iflytek.edcc.dataframe

import scala.collection.mutable.Set
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

case class bufferd(var list:Set[String], var num:Long)
case class rs(num:Long, count:Long)

class UDAFDistinctSafe extends Aggregator[Row, bufferd, Long]{

  override def zero: bufferd =  new bufferd(Set[String](), 0L)

  override def reduce(b: bufferd, a: Row): bufferd = {
    b.num += 1L
    b.list.add(a.getString(1))
    b
  }

  override def merge(b1: bufferd, b2: bufferd): bufferd = {
    b1.num += b2.num
    b1.list = b1.list ++ b2.list
    b1
  }

  override def finish(reduction: bufferd): Long = {
    reduction.list.size
  }

  override def bufferEncoder: Encoder[bufferd] = Encoders.javaSerialization

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
