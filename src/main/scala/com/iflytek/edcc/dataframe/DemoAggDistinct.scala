package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class user2(name:String, no:String)

object DemoAggDistinct {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("test")
      .getOrCreate()

    val data = sparkSession.createDataFrame(Array
      (
        user2("0001", "1"),
        user2("0001", "1"),
        user2("0001", "2"),
        user2("0002", "2"),
        user2("0003", "3")
      ))
    val uadfdistinct = new UDAFDistinct()
    val uadfdistinctsafe = new UDAFDistinctSafe().toColumn

    data
      .groupBy(col("name"))
      .agg(uadfdistinct(col("no")))
      .show()
//        .printSchema()

    data.groupBy(col("name"))
        .agg(uadfdistinctsafe)
        .show()

    sparkSession.stop()

  }

}
