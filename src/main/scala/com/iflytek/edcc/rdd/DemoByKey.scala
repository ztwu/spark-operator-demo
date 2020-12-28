package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

object DemoByKey {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"))

    //    为各个元素，按指定的函数生成key，形成key-value的RDD。
    rdd.keyBy(x=>{x.length}).foreach(x=>println(x))

  }

}
