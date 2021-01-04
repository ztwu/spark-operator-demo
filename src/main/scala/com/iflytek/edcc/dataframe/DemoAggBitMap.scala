package com.iflytek.edcc.dataframe

import com.iflytek.edcc.UdafBitMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class user(name:String, no:Int)

object DemoAggBitMap {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("test")
      .getOrCreate()

    val data = sparkSession.createDataFrame(Array
      (
        user("0001", 1),
        user("0001", 1),
        user("0001", 2),
        user("0002", 2),
        user("0003", 3)
      ))
    val bitmap = new UdafBitMap()
    val bitmap2 = new UDAFDistinctBitmap().toColumn

    data
      .groupBy(col("name"))
      .agg(bitmap(col("no")))
      .show()
//        .printSchema()

    data
      .groupBy(col("name"))
      .agg(bitmap2)
      .show()

    sparkSession.stop()

  }

}
