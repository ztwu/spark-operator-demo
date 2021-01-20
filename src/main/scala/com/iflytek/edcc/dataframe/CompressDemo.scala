package com.iflytek.edcc.dataframe

import org.apache.hadoop.io.SequenceFile
import org.apache.spark.sql.{SaveMode, SparkSession}

object CompressDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark-job")
      .getOrCreate
    val conf = spark.conf

    // text compress
    conf.set("mapreduce.output.fileoutputformat.compress", "true")
    conf.set("mapreduce.output.fileoutputformat.compress.type", SequenceFile.CompressionType.BLOCK.toString)
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
    conf.set("mapreduce.map.output.compress", "true")
    conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")

    val data = spark.createDataFrame(Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L)
    )) toDF("name", "age", "phone")

    data.select("name")
    .write
    .format("text")
    .mode(SaveMode.Overwrite)
    .save("bbbb")

    spark.stop()

  }
}
