package com.iflytek.edcc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 8:34
  * Description
  */

object DemoCogroup {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(("aa",1),("bb",2),("cc",6)))
    val rdd2 = sc.makeRDD(List(("aa",3),("dd",4),("aa",5)))

    val rdd = rdd1.cogroup(rdd2)
    rdd.foreach(println)

    sc.stop()

  }

}
