package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DemoAggHLL {
  case class user(name:String, no:Long)

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
//      .withColumn("no",col("no").cast(LongType))
    val udafhll = new UDAFHLL().toColumn
    data
      .groupBy(col("name"))
      .agg(udafhll)
      .show()
//        .printSchema()

    sparkSession.stop()

  }

}
