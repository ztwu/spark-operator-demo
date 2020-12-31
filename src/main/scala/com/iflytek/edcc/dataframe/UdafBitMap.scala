package com.iflytek.edcc.dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.roaringbitmap.RoaringBitmap
import java.io._
import java.util

/**
  * 实现自定义聚合函数Bitmap
  */
class UdafBitMap extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("field", DataTypes.BinaryType, true))
    DataTypes.createStructType(structFields)
  }

  override def bufferSchema: StructType = {
    val structFields = new util.ArrayList[StructField]
    structFields.add(DataTypes.createStructField("field", DataTypes.BinaryType, true))
    DataTypes.createStructType(structFields)
  }

  override def dataType: DataType = DataTypes.LongType

  override def deterministic: Boolean = { //是否强制每次执行的结果相同
    false
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = { //初始化
    buffer.update(0, null)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = { // 相同的executor间的数据合并
    // 1. 输入为空直接返回不更新
    val in = input.get(0)
    if (in == null) return
    // 2. 源为空则直接更新值为输入
    val inBytes = in.asInstanceOf[Array[Byte]]
    val out = buffer.get(0)
    if (out == null) {
      buffer.update(0, inBytes)
      return
    }
    // 3. 源和输入都不为空使用bitmap去重合并
    val outBytes = out.asInstanceOf[Array[Byte]]
    var result = outBytes
    val outRR = new RoaringBitmap
    val inRR = new RoaringBitmap
    try {
      outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)))
      inRR.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)))
      outRR.or(inRR)
      val bos = new ByteArrayOutputStream
      outRR.serialize(new DataOutputStream(bos))
      result = bos.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    buffer.update(0, result)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = { //不同excutor间的数据合并
    update(buffer1, buffer2)
  }

  override def evaluate(buffer: Row): Any = { //根据Buffer计算结果
    var r = 0l
    val `val` = buffer.get(0)
    if (`val` != null) {
      val rr = new RoaringBitmap
      try {
        rr.deserialize(new DataInputStream(new ByteArrayInputStream(`val`.asInstanceOf[Array[Byte]])))
        r = rr.getCardinality.toLong
      } catch {
        case e: IOException =>
          e.printStackTrace()
      }
    }
    r
  }
}
