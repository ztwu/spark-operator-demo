package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 14:03
  * Description
  */

object DemoUnion {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("01","ztwu1"),("02","ztwu2"),("03","ztwu3")))
    val rdd2 = sc.parallelize(List(("01","ztwu1"),("02","ztwu2")))

    //不去重
    rdd1.union(rdd2).foreach(println)

    sc.stop()

  }

}
