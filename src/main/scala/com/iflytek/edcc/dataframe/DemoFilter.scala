package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/6/4
  * Time: 11:13
  * Description
  */

object DemoFilter {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7))

    val dataframe = rdd.map(x=>{
      x
    }).toDF("user_id")

    dataframe.filter(dataframe("user_id")>3).foreach(x=>println(x))
    dataframe.filter($"user_id"<=3).foreach(x=>println(x.getAs[Int]("user_id")))

    sc.stop()


  }
}
