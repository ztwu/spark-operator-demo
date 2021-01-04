package com.iflytek.edcc.dataframe

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap

class UDAFDistinctBitmap extends Aggregator[Row, RoaringBitmap, Long]{

  override def zero: RoaringBitmap =  new RoaringBitmap()

  override def reduce(b: RoaringBitmap, a: Row): RoaringBitmap = {
    b.add(a.getInt(1))
    b
  }

  override def merge(b1: RoaringBitmap, b2: RoaringBitmap): RoaringBitmap = {
    b1.or(b2)
    b1
  }

  override def finish(reduction: RoaringBitmap): Long = {
    reduction.getLongCardinality
  }

  override def bufferEncoder: Encoder[RoaringBitmap] = Encoders.javaSerialization

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
