package com.iflytek.edcc.dataframe

import org.apache.hadoop.io.SequenceFile
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CompressDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark-job")
      .getOrCreate
    val conf = spark.conf
    import spark.implicits._

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

    val cols = data.columns
    var data2 = data
    for(col<-cols){
      data2 = data2.withColumn(s"new_${col}", concat_ws(":", lit(s"${col}"), data(s"${col}")))
    }
    data2 = data2.withColumn("array", concat_ws(",",col("new_name"),
      col("new_age"), col("new_phone")))
        .withColumn("split", split(col("array"),","))
        .select(col("name"), explode(col("split")) as "values")
        .withColumn("valuearray", split(col("values"),":"))
        .withColumn("key", col("valuearray").getItem(0))
        .withColumn("value", col("valuearray").getItem(1))
    data2.show()

    data.select("name")
    .write
    .format("text")
    .mode(SaveMode.Overwrite)
    .save("bbbb")

    spark.stop()

  }
}
