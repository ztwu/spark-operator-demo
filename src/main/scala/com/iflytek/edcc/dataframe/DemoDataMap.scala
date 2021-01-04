package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession

case class user(name:String, no:Int)

object DemoDataFrameMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("test")
      .getOrCreate()

//    定义出这个变量,导入他对应的implicits
    import spark.implicits._

    val data = spark.createDataFrame(Array
      (
        ("0001", 1),
        ("0001", 1),
        ("0001", 2),
        ("0002", 2),
        ("0003", 3)
      )).toDF("id","num")

    data.map(x=>{
      (x.getString(0),x.getInt(1))
    }).show()

    data.map(x=>{
      user(x.getString(0),x.getInt(1))
    }).show()

    spark.stop()

  }

}
