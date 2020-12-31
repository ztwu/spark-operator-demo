package com.iflytek.edcc.dataframe

import net.agkn.hll.HLL
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

class UDAFHLL extends Aggregator[Row, HLL, Long] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: HLL = new HLL(14, 5)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: HLL, user: Row): HLL = {
    buffer.addRaw(user.getLong(1))
    buffer
  }

  // Merge two intermediate values
  def merge(b1: HLL, b2: HLL): HLL = {
    b1.union(b2)
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: HLL): Long =  {
    reduction.cardinality()
  }

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[HLL] = Encoders.javaSerialization

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
