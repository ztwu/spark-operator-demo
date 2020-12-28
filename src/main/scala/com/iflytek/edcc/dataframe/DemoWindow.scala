package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 14:15
  * Description
  */

object DemoWindow {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
      //      .enableHiveSupport() //# sparkSQL 连接 hive 时需要这句
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val data = sc.makeRDD(Array(("01",1),("01",2),("02",1),("03",1)))
      .toDF("userid","num")

    // dataframe的开窗函数,需要配合hiveContext使用
    val window = Window.partitionBy("userid").orderBy(data("num").desc)
    val newSchoolData = data.withColumn("rn",row_number().over(window))
    newSchoolData.show()

    sc.stop()

  }

}
