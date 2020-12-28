package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DemoAggMy {
  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
      //      .enableHiveSupport() //# sparkSQL 连接 hive 时需要这句
      .getOrCreate()

    val ids = spark.range(1, 20)
    ids.createOrReplaceTempView("ids")
    val df = spark.sql("select id, id % 3 as group_id from ids")
    df.createOrReplaceTempView("simple")

    spark.udf.register("gm", new UDAFGeometricMean)
    spark.sql("select group_id, gm(id) from simple group by group_id").show()

    val gm = new UDAFGeometricMean
    // Show the geometric mean of values of column "id".
    df.groupBy("group_id").agg(gm(col("id")).as("GeometricMean")).show()
    // Invoke the UDAF by its assigned name.
    df.groupBy("group_id").agg(expr("gm(id) as GeometricMean")).show()

    spark.stop()

  }
}
