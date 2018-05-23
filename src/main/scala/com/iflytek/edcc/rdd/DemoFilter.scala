package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 13:44
  * Description
  */

object DemoFilter {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5,6))
    rdd.filter(x=>{
      x>4
    }).foreach(println)

  }

}
