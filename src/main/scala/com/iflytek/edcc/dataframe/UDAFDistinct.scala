package com.iflytek.edcc.dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StringType, StructField, StructType}

class UDAFDistinct extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
//  定义输入数据的Schema，要求类型是StructType，它的参数是由StructField类型构成的数组。
//  ::是Scala中的操作符与Nil空集合操作后生成一个数组。
  override def inputSchema: StructType =
    StructType(StructField("value", StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
//  事实上，计算score的平均值时，需要用到score的总和sum以及score的总个数count这样的中间数据，
//  那么就使用bufferSchema来定义它们。
  override def bufferSchema: StructType = StructType(
      StructField("num", LongType) ::
      StructField("list", ArrayType(StringType)) :: Nil
  )

  // This is the output type of your aggregatation function.
//  我们需要对自定义聚合函数的最终数据类型进行说明，使用dataType函数。
//  比如计算出的平均score是Double类型，
  override def dataType: DataType = LongType

//  deterministic函数用于对输入数据进行一致性检验，是一个布尔值，当为true时，表示对于同样的输入会得到同样的输出。
//  因为对于同样的score输入，肯定要得到相同的score平均值，所以定义为true
  override def deterministic: Boolean = false

  // This is the initial value for your buffer schema.
//  initialize用户初始化缓存数据。比如score的缓存数据有两个：sum和count
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = Seq[String]()
  }

  // This is how to update your buffer schema given an input.
//  当有新的输入数据时，update用户更新缓存变量
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getSeq[String](1) :+ input.getAs[String](0)
  }

  // This is how to merge two objects with the bufferSchema type.
//  merge将更新的缓存变量存入到缓存中
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getSeq[String](1) ++ buffer2.getSeq[String](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
//  evaluate是一个计算方法，用于计算我们的最终结果
  override def evaluate(buffer: Row): Long = {
    buffer.getSeq[String](1).toSet.size.toLong
  }
}
