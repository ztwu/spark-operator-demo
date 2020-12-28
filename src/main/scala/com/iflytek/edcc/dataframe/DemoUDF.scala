package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DemoUDF {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
      //      .enableHiveSupport() //# sparkSQL 连接 hive 时需要这句
      .getOrCreate()

    val squared = udf((s: Long) => {
      s * s
    })
//    注册方法
    spark.udf.register("square", squared)
    spark.range(1, 20).createOrReplaceTempView("test")
    spark.sql("select id, square(id) as id_squared from test").show()

//    调用方法
    spark.range(1, 20).select(squared(col("id")) as "id_squared").show()

  }

}
